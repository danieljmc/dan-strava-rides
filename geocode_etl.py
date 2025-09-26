# geocode_etl.py — reverse geocode (missing-only) with cache + partial flush
import os, json, time, math, re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import numpy as np
import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

PRINT_PREFIX = "[RG]"  # Reverse Geocode
TAB_ACTIVITIES   = "activities_all"
TAB_REV_CACHE    = "revgeo_cache"   # cache keyed by rounded lat/lon
TAB_GEO_BY_RIDE  = "geo_by_ride"    # per-activity result table

# ------------- Controls (env or GH inputs) -------------
RG_SINCE        = os.getenv("RG_SINCE", "").strip()         # YYYY-MM-DD (optional)
RG_MAX_RIDES    = int(os.getenv("RG_MAX_RIDES", "0"))       # 0 = no cap
RG_FLUSH_EVERY  = int(os.getenv("RG_FLUSH_EVERY", "25"))
RG_PLACES       = int(os.getenv("RG_PLACES", "3"))          # rounding places for cache key
RG_FORCE_REDO   = os.getenv("RG_FORCE_REDO", "0").lower() in ("1","true","yes")
RG_PROVIDER     = os.getenv("RG_PROVIDER", "opencage").lower()  # opencage | nominatim

OPENCAGE_KEY    = os.getenv("OPENCAGE_API_KEY", "").strip()
NOMINATIM_UA    = os.getenv("NOMINATIM_USER_AGENT", "strava-tableau-etl/1.0 (contact: danieljmc@comcast.net)")

THROTTLE_S      = float(os.getenv("RG_THROTTLE_S", "0.25"))  # Set to 1.0 for Nominatim
REQ_TIMEOUT_S   = int(os.getenv("RG_REQ_TIMEOUT_S", "30"))

# Secret fallback lat/lon (same pattern as weather_etl)
def _env_float(name):
    v = os.getenv(name)
    if v is None or str(v).strip()=="":
        return None
    try:
        return float(v)
    except:
        return None

def _env_float_any(names):
    for n in names:
        val = _env_float(n)
        if val is not None:
            return val, n
    return None, None

SECRET_LAT, lat_key = _env_float_any(["SECRET_LAT","HOME_LAT"])
SECRET_LON, lon_key = _env_float_any(["SECRET_LON","HOME_LON"])
SECRET_LATLON = (SECRET_LAT, SECRET_LON) if (SECRET_LAT is not None and SECRET_LON is not None) else None

# ------------- Sheets helpers -------------
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

def write_df_to_tab(sh, title, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+50,200)), cols=str(max(26, len(df.columns)+5)))
    ws.clear()
    if df.empty:
        headers = list(df.columns) if len(df.columns) else ["activity_id"]
        ws.update("A1", [headers]); ws.resize(rows=50, cols=max(5, len(headers)))
        print(f"{PRINT_PREFIX} wrote headers only to '{title}'."); return
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"{PRINT_PREFIX} wrote {len(df)} rows × {len(df.columns)} cols to '{title}'.")

# ------------- Lat/Lon parsing -------------
def _is_valid_latlon(lat, lon):
    try:
        lat=float(lat); lon=float(lon)
    except:
        return False
    if any([(lat!=lat), (lon!=lon)]):  # NaN
        return False
    return -90.0<=lat<=90.0 and -180.0<=lon<=180.0

def parse_latlon(val, fallback: Optional[Tuple[float,float]] = None) -> Optional[Tuple[float,float]]:
    if isinstance(val, (list, tuple)) and len(val)>=2:
        try:
            lt, ln = float(val[0]), float(val[1])
            return (lt, ln) if _is_valid_latlon(lt, ln) else fallback
        except:
            return fallback
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
            except:
                pass
        try:
            arr = json.loads(s)
            if isinstance(arr,(list,tuple)) and len(arr)>=2:
                return parse_latlon(arr, fallback=fallback)
        except:
            return fallback
    return fallback

# ------------- Providers -------------
def revgeo_opencage(lat, lon):
    if not OPENCAGE_KEY:
        raise RuntimeError("OPENCAGE_API_KEY is not set.")
    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {"q": f"{lat},{lon}", "key": OPENCAGE_KEY, "no_annotations": 1, "limit": 1}
    r = requests.get(url, params=params, timeout=REQ_TIMEOUT_S)
    r.raise_for_status()
    data = r.json()
    if not data.get("results"):
        return None
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
    if RG_PROVIDER == "nominatim":
        return revgeo_nominatim(lat, lon)
    return revgeo_opencage(lat, lon)

# ------------- Core -------------
def main():
    print(f"{PRINT_PREFIX} START reverse-geocode ETL (missing-only)")
    if SECRET_LATLON:
        print(f"{PRINT_PREFIX} Using SECRET fallback lat/lon from {lat_key}/{lon_key}.")
    else:
        print(f"{PRINT_PREFIX} No SECRET fallback; activities without coords will be skipped.")
    print(f"{PRINT_PREFIX} Provider={RG_PROVIDER}, places={RG_PLACES}, flush_every={RG_FLUSH_EVERY}")

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

    # Base eligibility: must have id and lat/lon
    base = df_acts[df_acts["activity_id"].notna() & df_acts["lat"].notna() & df_acts["lon"].notna()].copy()

    # Read existing tabs
    df_cache = read_tab_to_df(sh, TAB_REV_CACHE)
    df_geo   = read_tab_to_df(sh, TAB_GEO_BY_RIDE)

    # Build rounded keys
    def rnd(x):
        try: return round(float(x), RG_PLACES)
        except: return np.nan
    base["lat_round"] = base["lat"].apply(rnd)
    base["lon_round"] = base["lon"].apply(rnd)

    # Anti-join for cache lookups we still need
    if not df_cache.empty:
        need_cache = base[["lat_round","lon_round"]].drop_duplicates()
        cur = df_cache[["lat_round","lon_round"]]
        need_cache = (need_cache.merge(cur, on=["lat_round","lon_round"], how="left", indicator=True)
                                 .query("_merge=='left_only'")
                                 [["lat_round","lon_round"]])
    else:
        need_cache = base[["lat_round","lon_round"]].drop_duplicates()

    # Build list of activity_ids we still need in geo_by_ride
    if not df_geo.empty and not RG_FORCE_REDO:
        done_ids = set(pd.to_numeric(df_geo["activity_id"], errors="coerce").dropna().astype(int).tolist())
    else:
        done_ids = set()
    target = base[~base["activity_id"].astype(int).isin(done_ids)].copy()
    if RG_MAX_RIDES > 0:
        target = target.sort_values("start_date_local", ascending=False).head(RG_MAX_RIDES)

    print(f"{PRINT_PREFIX} cache-miss lat/lon pairs to look up: {len(need_cache)}")
    print(f"{PRINT_PREFIX} activities to add to geo_by_ride: {len(target)} (force_redo={int(RG_FORCE_REDO)})")

    # --- Fill cache (respect provider rate limits) ---
    new_cache_rows = []
    for i, row in enumerate(need_cache.itertuples(index=False), start=1):
        lat_r, lon_r = float(row.lat_round), float(row.lon_round)
        try:
            info = reverse_geocode(lat_r, lon_r)
        except Exception as e:
            print(f"{PRINT_PREFIX} provider error at ({lat_r},{lon_r}): {str(e)[:120]}")
            info = None
        new_cache_rows.append({
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

    if new_cache_rows:
        df_cache_new = pd.DataFrame(new_cache_rows)
        df_cache_all = pd.concat([df_cache, df_cache_new], ignore_index=True)
        df_cache_all = df_cache_all.drop_duplicates(subset=["lat_round","lon_round"], keep="last")
    else:
        df_cache_all = df_cache.copy()

    # Ensure expected cache columns exist (even if empty/new)
    EXPECTED_CACHE_COLS = ["lat_round","lon_round","city","state","country","display_name","source","updated_at"]
    for c in EXPECTED_CACHE_COLS:
        if c not in df_cache_all.columns:
            df_cache_all[c] = pd.Series(dtype="object")

    # --- helper: single flush ---
    def flush(df_cache_all_local, geo_new_local, i=None, total=None):
        if i is not None:
            print(f"{PRINT_PREFIX} flushing {i}/{total} …")
        write_df_to_tab(sh, TAB_REV_CACHE, df_cache_all_local)
        # Combine existing geo_by_ride with new; dedupe by activity_id
        if not df_geo.empty or not geo_new_local.empty:
            geo_comb = pd.concat([df_geo, geo_new_local], ignore_index=True)
            if not geo_comb.empty:
                geo_comb["activity_id"] = pd.to_numeric(geo_comb["activity_id"], errors="coerce").astype("Int64")
                geo_comb = geo_comb.drop_duplicates(subset=["activity_id"], keep="last")
        else:
            geo_comb = geo_new_local
        write_df_to_tab(sh, TAB_GEO_BY_RIDE, geo_comb)

    # --- Build geo_by_ride rows (chunked or single-shot) ---
    GEO_COLS = ["activity_id","lat","lon","city","state","country","display_name","lat_round","lon_round"]

    if len(target) > 0 and RG_FLUSH_EVERY > 0 and len(target) > RG_FLUSH_EVERY:
        total = len(target)
        chunks = [target.iloc[i:i+RG_FLUSH_EVERY] for i in range(0, total, RG_FLUSH_EVERY)]
        for idx, chunk in enumerate(chunks, start=1):
            merged = chunk.merge(df_cache_all, on=["lat_round","lon_round"], how="left")
            geo_chunk = merged.reindex(columns=GEO_COLS)
            flush(df_cache_all, geo_chunk, i=min(idx*RG_FLUSH_EVERY, total), total=total)
    else:
        merged = target.merge(df_cache_all, on=["lat_round","lon_round"], how="left")
        geo_new = merged.reindex(columns=GEO_COLS)
        flush(df_cache_all, geo_new)

    print(f"{PRINT_PREFIX} DONE. Provider={RG_PROVIDER}. Tabs updated: {TAB_REV_CACHE}, {TAB_GEO_BY_RIDE}")

if __name__ == "__main__":
    main()
