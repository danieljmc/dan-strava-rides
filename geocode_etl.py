# geocode_etl.py — reverse geocode (missing-only) with cache, single-write
# optional time-based checkpoints, and 429 backoff on Sheets writes.

import os, json, time, re
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np
import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

PRINT_PREFIX = "[RG]"
TAB_ACTIVITIES   = "activities_all"
TAB_REV_CACHE    = "revgeo_cache"   # cache keyed by rounded lat/lon
TAB_GEO_BY_RIDE  = "geo_by_ride"    # per-activity result table

# -------- Controls (env or GH inputs) --------
RG_SINCE        = os.getenv("RG_SINCE", "").strip()         # YYYY-MM-DD (optional)
RG_MAX_RIDES    = int(os.getenv("RG_MAX_RIDES", "0"))       # 0 = no cap
RG_PLACES       = int(os.getenv("RG_PLACES", "3"))          # rounding places
RG_FORCE_REDO   = os.getenv("RG_FORCE_REDO", "0").lower() in ("1","true","yes")
RG_PROVIDER     = os.getenv("RG_PROVIDER", "opencage").lower()  # opencage | nominatim
# Geocoder pacing
THROTTLE_S      = float(os.getenv("RG_THROTTLE_S", "0.25"))      # set 1.0 for Nominatim
REQ_TIMEOUT_S   = int(os.getenv("RG_REQ_TIMEOUT_S", "30"))
# Optional checkpointing: write to Sheets every N minutes (0 = end only)
RG_CHECKPOINT_MIN = float(os.getenv("RG_CHECKPOINT_MIN", "0"))    # e.g., 2.0

OPENCAGE_KEY    = os.getenv("OPENCAGE_API_KEY", "").strip()
NOMINATIM_UA    = os.getenv("NOMINATIM_USER_AGENT", "strava-tableau-etl/1.0 (contact: you@example.com)")

# -------- Secret fallback lat/lon (same pattern as weather) --------
def _env_float(name):
    v = os.getenv(name)
    if v is None or str(v).strip()=="":
        return None
    try: return float(v)
    except: return None

def _env_float_any(names):
    for n in names:
        val = _env_float(n)
        if val is not None: return val, n
    return None, None

SECRET_LAT, lat_key = _env_float_any(["SECRET_LAT","HOME_LAT"])
SECRET_LON, lon_key = _env_float_any(["SECRET_LON","HOME_LON"])
SECRET_LATLON = (SECRET_LAT, SECRET_LON) if (SECRET_LAT is not None and SECRET_LON is not None) else None

# -------- Sheets helpers --------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def read_tab_to_df(sh, title) -> pd.DataFrame:
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    try:
        rows = ws.get_all_records(numeric_value_strategy="RAW")
    except TypeError:
        rows = ws.get_all_records()
    return pd.DataFrame(rows)

def _write_with_backoff(fn_desc: str, func, *args, **kwargs):
    # Exponential backoff for Sheets 429s
    backoff = 2.0
    for attempt in range(7):  # ~2+4+8+16+32+64 ~ 126s max
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if "429" in str(e):
                sleep_s = backoff
                print(f"{PRINT_PREFIX} Sheets 429 on {fn_desc}; backing off {sleep_s:.0f}s …")
                time.sleep(sleep_s)
                backoff = min(backoff*2, 64)
            else:
                raise
    # last try without catching
    return func(*args, **kwargs)

def write_df_to_tab(sh, title, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="200", cols="26")

    # Clear once, then write
    _write_with_backoff(f"clear({title})", ws.clear)
    if df.empty:
        headers = list(df.columns) if len(df.columns) else ["activity_id"]
        _write_with_backoff(f"write-headers({title})", ws.update, "A1", [headers])
        _write_with_backoff(f"resize({title})", ws.resize, rows=50, cols=max(5, len(headers)))
        print(f"{PRINT_PREFIX} wrote headers only to '{title}'.")
        return

    # Use gspread_dataframe helper
    def _set_df():
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    _write_with_backoff(f"set_with_dataframe({title})", _set_df)
    print(f"{PRINT_PREFIX} wrote {len(df)} rows × {len(df.columns)} cols to '{title}'.")

# -------- Lat/Lon parsing --------
def _is_valid_latlon(lat, lon):
    try:
        lat=float(lat); lon=float(lon)
    except: return False
    if any([(lat!=lat), (lon!=lon)]): return False  # NaN
    return -90.0<=lat<=90.0 and -180.0<=lon<=180.0

def parse_latlon(val, fallback: Optional[Tuple[float,float]] = None) -> Optional[Tuple[float,float]]:
    if isinstance(val, (list, tuple)) and len(val)>=2:
        try:
            lt, ln = float(val[0]), float(val[1])
            return (lt, ln) if _is_valid_latlon(lt, ln) else fallback
        except: return fallback
    if val is None: return fallback
    if isinstance(val, str):
        s = val.strip()
        if s in ("","[]","null","None","NaN","nan"): return fallback
        core = s.strip("[]() ")
        parts = [p.strip() for p in core.split(",") if p.strip()!=""]
        if len(parts)>=2:
            try:
                lt, ln = float(parts[0]), float(parts[1])
                if _is_valid_latlon(lt, ln): return (lt, ln)
            except: pass
        try:
            arr = json.loads(s)
            if isinstance(arr,(list,tuple)) and len(arr)>=2:
                return parse_latlon(arr, fallback=fallback)
        except: return fallback
    return fallback

# -------- Providers --------
def revgeo_opencage(lat, lon):
    if not OPENCAGE_KEY:
        raise RuntimeError("OPENCAGE_API_KEY is not set.")
    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {"q": f"{lat},{lon}", "key": OPENCAGE_KEY, "no_annotations": 1, "limit": 1}
    r = requests.get(url, params=params, timeout=REQ_TIMEOUT_S)
    r.raise_for_status()
    data = r.json()
    if not data.get("results"): return None
    res = data["results"][0]
    c = res.get("components", {})
    city = c.get("city") or c.get("town") or c.get("village") or c.get("hamlet")
    state = c.get("state")
    country = c.get("country")
    label = res.get("formatted")
    return {"city": city, "state": state, "country": country, "display_name": label, "source": "OpenCage"}

def revgeo_nominatim(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "jsonv2", "zoom": 10, "addressdetails": 1}
    headers = {"User-Agent": NOMINATIM_UA}
    r = requests.get(url, params=params, headers=headers, timeout=REQ_TIMEOUT_S)
    r.raise_for_status()
    data = r.json()
    addr = data.get("address", {})
    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet")
    state = addr.get("state")
    country = addr.get("country")
    label = data.get("display_name")
    return {"city": city, "state": state, "country": country, "display_name": label, "source": "Nominatim"}

def reverse_geocode(lat, lon):
    return revgeo_nominatim(lat, lon) if RG_PROVIDER == "nominatim" else revgeo_opencage(lat, lon)

# -------- Core --------
def main():
    print(f"{PRINT_PREFIX} START reverse-geocode ETL (missing-only)")
    if SECRET_LATLON:
        print(f"{PRINT_PREFIX} Using SECRET fallback lat/lon from {lat_key}/{lon_key}.")
    else:
        print(f"{PRINT_PREFIX} No SECRET fallback; activities without coords will be skipped.")
    print(f"{PRINT_PREFIX} Provider={RG_PROVIDER}, places={RG_PLACES}")

    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    df_acts = read_tab_to_df(sh, TAB_ACTIVITIES)
    if df_acts.empty:
        print(f"{PRINT_PREFIX} '{TAB_ACTIVITIES}' is empty; nothing to do."); return

    # Ensure required columns exist
    need = {"id","start_latlng","start_date_local"}
    missing = [c for c in need if c not in df_acts.columns]
    if missing:
        raise ValueError(f"Missing required column(s) in {TAB_ACTIVITIES}: {missing}")

    # Parse lat/lon with fallback
    latlons = df_acts["start_latlng"].apply(lambda v: parse_latlon(v, fallback=SECRET_LATLON))
    df_acts["lat"] = latlons.apply(lambda t: t[0] if t else np.nan)
    df_acts["lon"] = latlons.apply(lambda t: t[1] if t else np.nan)
    df_acts["activity_id"] = pd.to_numeric(df_acts["id"], errors="coerce").astype("Int64")

    # Optional time scope
    if RG_SINCE:
        since_dt = pd.to_datetime(RG_SINCE, errors="coerce")
        if pd.notna(since_dt):
            df_acts = df_acts[pd.to_datetime(df_acts["start_date_local"], errors="coerce") >= since_dt]

    # Eligible rows
    base = df_acts[df_acts["activity_id"].notna() & df_acts["lat"].notna() & df_acts["lon"].notna()].copy()

    # Existing tabs
    df_cache = read_tab_to_df(sh, TAB_REV_CACHE)
    df_geo   = read_tab_to_df(sh, TAB_GEO_BY_RIDE)

    # Rounded keys
    def rnd(x):
        try: return round(float(x), RG_PLACES)
        except: return np.nan
    base["lat_round"] = base["lat"].apply(rnd)
    base["lon_round"] = base["lon"].apply(rnd)

    # Determine cache misses
    if not df_cache.empty:
        need_cache = base[["lat_round","lon_round"]].drop_duplicates()
        cur = df_cache[["lat_round","lon_round"]]
        need_cache = (need_cache.merge(cur, on=["lat_round","lon_round"], how="left", indicator=True)
                                 .query("_merge=='left_only'")
                                 [["lat_round","lon_round"]])
    else:
        need_cache = base[["lat_round","lon_round"]].drop_duplicates()

    # Determine which activity_ids still need geo
    if not df_geo.empty and not RG_FORCE_REDO:
        done_ids = set(pd.to_numeric(df_geo["activity_id"], errors="coerce").dropna().astype(int).tolist())
    else:
        done_ids = set()
    target = base[~base["activity_id"].astype(int).isin(done_ids)].copy()
    if RG_MAX_RIDES > 0:
        target = target.sort_values("start_date_local", ascending=False).head(RG_MAX_RIDES)

    print(f"{PRINT_PREFIX} cache-miss lat/lon pairs to look up: {len(need_cache)}")
    print(f"{PRINT_PREFIX} activities to add to geo_by_ride: {len(target)} (force_redo={int(RG_FORCE_REDO)})")

    # ---- Fill cache in memory ----
    cache_rows: List[dict] = []
    for i, row in enumerate(need_cache.itertuples(index=False), start=1):
        lat_r, lon_r = float(row.lat_round), float(row.lon_round)
        try:
            info = reverse_geocode(lat_r, lon_r)
        except Exception as e:
            print(f"{PRINT_PREFIX} provider error at ({lat_r},{lon_r}): {str(e)[:140]}")
            info = None
        cache_rows.append({
            "lat_round": lat_r, "lon_round": lon_r,
            "city": (info or {}).get("city"),
            "state": (info or {}).get("state"),
            "country": (info or {}).get("country"),
            "display_name": (info or {}).get("display_name"),
            "source": (info or {}).get("source") or RG_PROVIDER,
            "updated_at": datetime.utcnow().isoformat(timespec="seconds")+"Z"
        })
        if i % 25 == 0:
            print(f"{PRINT_PREFIX} cache progress {i}/{len(need_cache)} …")
        time.sleep(THROTTLE_S)

    # Build df_cache_all (in-memory)
    if cache_rows:
        df_cache_new = pd.DataFrame(cache_rows)
        df_cache_all = pd.concat([df_cache, df_cache_new], ignore_index=True)
        df_cache_all = df_cache_all.drop_duplicates(subset=["lat_round","lon_round"], keep="last")
    else:
        df_cache_all = df_cache.copy()

    # Ensure expected cache cols exist
    EXPECTED_CACHE_COLS = ["lat_round","lon_round","city","state","country","display_name","source","updated_at"]
    for c in EXPECTED_CACHE_COLS:
        if c not in df_cache_all.columns:
            df_cache_all[c] = pd.Series(dtype="object")

    # ---- Build geo_by_ride in memory (and optional time checkpoints) ----
    GEO_COLS = ["activity_id","lat","lon","city","state","country","display_name","lat_round","lon_round"]
    geo_accum = []  # list of DataFrames to concatenate once
    last_checkpoint = datetime.utcnow()

    if len(target) > 0:
        # iterate in small chunks to bound memory if very large
        CHUNK = 500
        for start in range(0, len(target), CHUNK):
            chunk = target.iloc[start:start+CHUNK]
            merged = chunk.merge(df_cache_all, on=["lat_round","lon_round"], how="left")
            geo_chunk = merged.reindex(columns=GEO_COLS)
            geo_accum.append(geo_chunk)

            # Optional time-based checkpoint write (disabled if RG_CHECKPOINT_MIN==0)
            if RG_CHECKPOINT_MIN > 0:
                if datetime.utcnow() - last_checkpoint >= timedelta(minutes=RG_CHECKPOINT_MIN):
                    print(f"{PRINT_PREFIX} checkpoint write …")
                    # Combine existing + accumulated and write once
                    geo_comb = pd.concat([df_geo] + geo_accum, ignore_index=True) if geo_accum else df_geo
                    if not geo_comb.empty:
                        geo_comb["activity_id"] = pd.to_numeric(geo_comb["activity_id"], errors="coerce").astype("Int64")
                        geo_comb = geo_comb.drop_duplicates(subset=["activity_id"], keep="last")
                    _write_with_backoff("revgeo_cache write", write_df_to_tab, sh, TAB_REV_CACHE, df_cache_all)
                    _write_with_backoff("geo_by_ride write", write_df_to_tab, sh, TAB_GEO_BY_RIDE, geo_comb)
                    last_checkpoint = datetime.utcnow()
    else:
        geo_accum.append(pd.DataFrame(columns=GEO_COLS))

    # ---- Final single write to Sheets ----
    geo_final = pd.concat([df_geo] + geo_accum, ignore_index=True) if geo_accum else df_geo
    if not geo_final.empty:
        geo_final["activity_id"] = pd.to_numeric(geo_final["activity_id"], errors="coerce").astype("Int64")
        geo_final = geo_final.drop_duplicates(subset=["activity_id"], keep="last")

    print(f"{PRINT_PREFIX} writing to Sheets (single write)…")
    _write_with_backoff("revgeo_cache write", write_df_to_tab, sh, TAB_REV_CACHE, df_cache_all)
    _write_with_backoff("geo_by_ride write", write_df_to_tab, sh, TAB_GEO_BY_RIDE, geo_final)

    print(f"{PRINT_PREFIX} DONE. Provider={RG_PROVIDER}. Tabs updated: {TAB_REV_CACHE}, {TAB_GEO_BY_RIDE}")

if __name__ == "__main__":
    main()
