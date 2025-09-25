# pipeline.py  (incremental + weather cache) — fixed tz-compare + concat warnings + 'H' deprecation
import os, json, time, re, math
from datetime import timedelta
import requests
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

print("=== PIPELINE START ===")
print("PIPELINE VERSION: inc+wx v1")

# ---------------- Config ----------------
PER_PAGE = 200  # Strava's max
THROTTLE_S = 0.2
ADD_GEAR_NAME = True
INCLUDE_SOLO_PLACEHOLDER = True

# Tabs
TAB_ACTIVITIES   = "activities_all"   # wide activities
TAB_RIDERS_LONG  = "riders_long"      # (activity_id, rider_name)
TAB_WX_CACHE     = "weather_cache"    # hourly weather cache

# Weather cache settings
LATLON_ROUND = 1  # decimal places to round lat/lon for cache key (1 ≈ ~11 km)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HOURLY = "temperature_2m,cloudcover,windspeed_10m,winddirection_10m,weathercode"

# ---------------- Google Sheets I/O ----------------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    gc = gspread_client_from_secret()
    return gc.open_by_key(os.environ["SHEET_ID"])

def read_tab_df(sh, title: str) -> pd.DataFrame:
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    # try numeric conversion where possible
    for c in df.columns:
        if df[c].dtype == object:
            try:
                df[c] = pd.to_numeric(df[c])
            except Exception:
                pass
    return df

def write_df_to_tab(sh, title: str, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max((len(df) if df is not None else 0)+200, 200)), cols=str(max(26, (len(df.columns) if df is not None else 1)+5)))
    ws.clear()
    if df is None or df.empty:
        headers = list(df.columns) if df is not None and len(df.columns)>0 else ["_empty"]
        ws.update("A1", [headers])
        ws.resize(rows=50, cols=max(5, len(headers)))
        print(f"Wrote 0 rows to tab '{title}' (headers only).")
        return
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"Wrote {len(df)} rows x {len(df.columns)} cols to tab '{title}'.")

# ---------------- Strava auth + fetch ----------------
def get_strava_access_token():
    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": os.environ["STRAVA_CLIENT_ID"],
            "client_secret": os.environ["STRAVA_CLIENT_SECRET"],
            "grant_type": "refresh_token",
            "refresh_token": os.environ["STRAVA_REFRESH_TOKEN"],
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    print("Strava token scope:", data.get("scope"))
    return data["access_token"]

def fetch_all_activities(access_token):
    all_rows, page = [], 1
    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": PER_PAGE, "page": page},
            timeout=60,
        )
        if resp.status_code == 429:
            print("Rate limited; sleeping 60s..."); time.sleep(60); continue
        resp.raise_for_status()
        batch = resp.json()
        if not batch: break
        all_rows.extend(batch)
        print(f"Fetched page {page} -> {len(batch)} activities (total {len(all_rows)}).")
        page += 1
        time.sleep(THROTTLE_S)
    return all_rows

def fetch_gear_map(access_token):
    if not ADD_GEAR_NAME:
        return {}
    resp = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    m = {}
    for bike in (data.get("bikes") or []):
        if bike.get("id"):
            m[bike["id"]] = bike.get("name") or bike.get("brand_name") or "Bike"
    for shoe in (data.get("shoes") or []):
        if shoe.get("id"):
            m[shoe["id"]] = shoe.get("name") or shoe.get("brand_name") or "Shoes"
    return m

# ---------------- Riders parsing (strict "with" only) ----------------
WITH_PATTERN = re.compile(r"\bwith\b", flags=re.IGNORECASE)

STOPWORDS = {
    # groups / family
    "family","boys","dad","mom","wife","husband","kids","scouts","troop","group","team","party","committee","club",
    # misc generic
    "home",
    # local places / common words
    "washington","tiverton","fall","river","sepowet","boy","scout","hill","buck","secondary",
    # activity words
    "solo","loop","ride","walk","hike","skate","skating","biking","cycling","trainer","activity",
    "morning","afternoon","evening","lunch","first","return","reverse","leisurely","slow",
    "rd","road","street","blvd","park","path","trail","drive","point","beach","lake","falls","notch",
    "green","bridge","bridges","campground","maze","summit",
    "north","south","east","west",
    "ebbp","nh","ri","fl",
    "newport","providence","sakonnet","sakonnett","acoaxet","bulgamarsh","horseneck",
    "padenarem","padanaram","dartmouth","westport","adamsville","seapowet","blackstone",
    "bristol","portsmouth","lincoln","waterville","valley","kancamagus","conway","taunton",
}

KEEP_PAREN_PHRASES = False

def strip_parens(t: str) -> str:
    t = t.strip()
    if t.startswith("(") and t.endswith(")"):
        inner = t[1:-1].strip()
        return inner if KEEP_PAREN_PHRASES else ""
    return t

def clean_token(w: str) -> str:
    w = w.strip()
    w = re.sub(r"^[^A-Za-z]+|[^A-Za-z]+$", "", w)
    return w

def is_name_token(tok: str) -> bool:
    if not tok:
        return False
    if any(ch.isdigit() for ch in tok):
        return False
    lt = tok.lower()
    if lt in STOPWORDS:
        return False
    return tok[0].isupper() and tok[1:].islower()

def split_by_commas_and_and(s: str) -> list[str]:
    s = s.replace("&", " and ")
    s = re.sub(r"\s+and\s+", ",", s, flags=re.IGNORECASE)
    return [p.strip() for p in s.split(",") if p.strip()]

def tokenize_names_after_with(segment: str) -> list[str]:
    out = []
    for part in split_by_commas_and_and(segment):
        core = strip_parens(part)
        for w in core.split():
            w = clean_token(w)
            if not w: continue
            if is_name_token(w):
                out.append(w.title())
    seen, uniq = set(), []
    for t in out:
        if t not in seen:
            uniq.append(t); seen.add(t)
    return uniq

def extract_riders_from_name(activity_name: str) -> list[str]:
    if not activity_name:
        return []
    m = WITH_PATTERN.search(activity_name)
    if not m:
        return []
    after = activity_name[m.end():]
    return tokenize_names_after_with(after)

# ---------------- Weather helpers ----------------
def round_latlon(lat, lon, places=LATLON_ROUND):
    return (round(float(lat), places), round(float(lon), places))

def om_fetch_hourly(lat, lon, date_iso):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": OPEN_METEO_HOURLY,
        "start_date": date_iso,
        "end_date": date_iso,
        "timezone": "auto",
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    if r.status_code == 429:
        print("Open-Meteo rate limited; sleeping 60s..."); time.sleep(60)
        r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def circular_mean_deg(deg_list):
    if not deg_list:
        return None
    rad = np.deg2rad(deg_list)
    s = np.mean(np.sin(rad))
    c = np.mean(np.cos(rad))
    mean = math.degrees(math.atan2(s, c))
    if mean < 0:
        mean += 360.0
    return mean

def wx_condition_from(hour_cc_list, hour_codes):
    rainy_codes = {51,53,55,56,57,61,63,65,66,67,80,81,82}
    snowy_codes = {71,73,75,77,85,86}
    if any(code in snowy_codes for code in hour_codes):
        return "Snowy"
    if any(code in rainy_codes for code in hour_codes):
        return "Rainy"
    avg_cc = np.mean(hour_cc_list) if hour_cc_list else 0.0
    return "Cloudy" if avg_cc >= 50 else "Sunny"

# ---------------- Normalize activities (summary) ----------------
def normalize_activities(acts, gear_map):
    df = pd.json_normalize(acts, sep=".")
    # Datetimes
    if "start_date_local" in df.columns:
        dt = pd.to_datetime(df["start_date_local"], errors="coerce")
        df["start_date_local_fmt"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    if "start_date" in df.columns:
        dt2 = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date_fmt"] = dt2.dt.strftime("%Y-%m-%d %H:%M:%S")
    # Gear
    if "gear_id" in df.columns:
        df["gear_name"] = df["gear_id"].map(lambda g: gear_map.get(g) if gear_map else None)
    # Riders
    if "name" in df.columns and "id" in df.columns:
        riders_lists = df["name"].apply(extract_riders_from_name)
        df["other_riders"] = riders_lists.apply(lambda lst: ", ".join(lst) if lst else "")
        df["other_riders_count"] = riders_lists.apply(len)
        df["other_riders_json"] = riders_lists.apply(lambda lst: json.dumps(lst) if lst else "[]")
    # Sort
    sort_col = "start_date_local" if "start_date_local" in df.columns else ("start_date" if "start_date" in df.columns else None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False, kind="mergesort")
    return df

def build_riders_long(df_acts: pd.DataFrame) -> pd.DataFrame:
    cols = ["activity_id", "rider_name"]
    if df_acts.empty or "id" not in df_acts.columns:
        return pd.DataFrame(columns=cols)
    rows = []
    has_cols = "other_riders" in df_acts.columns and "other_riders_count" in df_acts.columns
    if has_cols:
        for act_id, riders, cnt in zip(df_acts["id"], df_acts["other_riders"], df_acts["other_riders_count"]):
            if cnt and riders:
                for rider in [r.strip() for r in riders.split(",") if r.strip()]:
                    rows.append({"activity_id": int(act_id), "rider_name": rider})
    # Solo placeholder
    if INCLUDE_SOLO_PLACEHOLDER:
        solo_ids = df_acts.loc[df_acts.get("other_riders_count", pd.Series([0]*len(df_acts))).fillna(0) == 0, "id"]
        for act_id in solo_ids:
            rows.append({"activity_id": int(act_id), "rider_name": " Solo"})
    df = pd.DataFrame(rows, columns=cols)
    return df

# ---------------- Incremental merge helpers ----------------
def merge_incremental(existing: pd.DataFrame, incoming: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (combined_df, new_rows_df)
    - existing: current activities_all from Sheets
    - incoming: fresh from Strava
    """
    if existing is None or existing.empty:
        return incoming.copy(), incoming.copy()

    for df in (existing, incoming):
        if "id" in df.columns:
            df["id"] = pd.to_numeric(df["id"], errors="coerce")

    existing_ids = set(existing["id"].dropna().astype(int).tolist()) if "id" in existing.columns else set()

    if incoming is None or incoming.empty:
        # Nothing incoming; no new rows
        combined = existing.copy()
        new_rows = incoming.copy() if incoming is not None else pd.DataFrame(columns=existing.columns)
        return combined, new_rows

    incoming["is_new"] = ~incoming["id"].astype("Int64").isin(existing_ids)
    new_rows = incoming[incoming["is_new"]].drop(columns=["is_new"]).copy()

    # Avoid pandas concat empty/all-NA warnings by filtering
    frames = []
    if existing is not None and not existing.empty:
        frames.append(existing)
    if new_rows is not None and not new_rows.empty:
        frames.append(new_rows)
    if frames:
        combined = pd.concat(frames, ignore_index=True, sort=False)
    else:
        combined = incoming.head(0).copy()
    return combined, new_rows

# ---------------- Weather cache load/save ----------------
def load_weather_cache(sh) -> pd.DataFrame:
    df = read_tab_df(sh, TAB_WX_CACHE)
    if df.empty:
        return pd.DataFrame(columns=[
            "date", "lat_round", "lon_round",
            "time_iso", "temperature_2m", "cloudcover", "windspeed_10m", "winddirection_10m", "weathercode"
        ])
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "time_iso" in df.columns:
        df["time_iso"] = df["time_iso"].astype(str)
    for col in ["lat_round","lon_round","temperature_2m","cloudcover","windspeed_10m","winddirection_10m","weathercode"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def append_to_weather_cache(cache_df: pd.DataFrame, lat_r: float, lon_r: float, date_iso: str, hourly: dict) -> pd.DataFrame:
    times = hourly.get("time", []) or []
    temps = hourly.get("temperature_2m", []) or []
    cc    = hourly.get("cloudcover", []) or []
    ws    = hourly.get("windspeed_10m", []) or []
    wd    = hourly.get("winddirection_10m", []) or []
    wc    = hourly.get("weathercode", []) or []
    rows = []
    for t, T, C, S, D, W in zip(times, temps, cc, ws, wd, wc):
        rows.append({
            "date": pd.to_datetime(date_iso).date(),
            "lat_round": lat_r, "lon_round": lon_r,
            "time_iso": t,
            "temperature_2m": T,
            "cloudcover": C,
            "windspeed_10m": S,
            "winddirection_10m": D,
            "weathercode": W,
        })
    add_df = pd.DataFrame(rows)
    if add_df.empty:
        return cache_df
    if cache_df is None or cache_df.empty:
        updated = add_df.copy()
    else:
        updated = pd.concat([cache_df, add_df], ignore_index=True)
    return updated

def ensure_hour_in_cache(cache_df: pd.DataFrame, lat_r: float, lon_r: float, date_iso: str) -> pd.DataFrame:
    sub = cache_df[(cache_df["lat_round"] == lat_r) & (cache_df["lon_round"] == lon_r) & (cache_df["date"] == pd.to_datetime(date_iso).date())]
    have_hours = len(sub)
    if have_hours >= 24:
        return cache_df
    data = om_fetch_hourly(lat_r, lon_r, date_iso)
    hourly = data.get("hourly", {})
    cache_df = append_to_weather_cache(cache_df, lat_r, lon_r, date_iso, hourly)
    time.sleep(0.2)
    return cache_df

# ---- Datetime normalization helper (prevents tz-aware vs tz-naive crash) ----
def _to_naive_datetime_series(s: pd.Series) -> pd.Series:
    """
    Parse timestamps (tz-aware or naive) -> UTC -> strip tz, returning tz-naive.
    This makes them comparable to tz-naive start/end from start_date_local.
    """
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    return dt.dt.tz_convert(None)

# ---------------- Weather enrichment per activity ----------------
def parse_start_latlon(row):
    v = row.get("start_latlng", None)
    if isinstance(v, list) and len(v) == 2:
        return float(v[0]), float(v[1])
    if isinstance(v, str) and "," in v:
        try:
            lat_s, lon_s = v.split(",", 1)
            return float(lat_s.strip()), float(lon_s.strip())
        except Exception:
            pass
    home_lat = float(os.getenv("HOME_LAT", "41.636"))
    home_lon = float(os.getenv("HOME_LON", "-71.176"))
    return home_lat, home_lon

def compute_wx_for_activity(row, cache_df: pd.DataFrame):
    # Start/end time in LOCAL (tz-naive from Strava); keep tz-naive consistently.
    start = pd.to_datetime(row.get("start_date_local"), errors="coerce")
    if pd.isna(start):
        return None
    secs = pd.to_numeric(row.get("elapsed_time"), errors="coerce")
    if pd.isna(secs):
        return None
    end = start + timedelta(seconds=float(secs))

    lat, lon = parse_start_latlon(row)
    lat_r, lon_r = round_latlon(lat, lon)

    # Which dates to cover (handles rides that cross midnight)
    days = pd.date_range(start.normalize(), end.normalize(), freq="D").date

    # Ensure cache has all needed hourly rows for each date
    for d in days:
        cache_df = ensure_hour_in_cache(cache_df, lat_r, lon_r, pd.Timestamp(d).date().isoformat())

    # Pull relevant hours subset across dates within [start, end]
    mask = (
        (cache_df["lat_round"] == lat_r) &
        (cache_df["lon_round"] == lon_r) &
        (cache_df["date"].isin(days))
    )
    subset = cache_df.loc[mask].copy()
    if subset.empty:
        return None

    # Normalize cache times to tz-naive to match start/end; also handle strings with offsets
    subset["t"] = _to_naive_datetime_series(subset["time_iso"])

    # Use lowercase 'h' (pandas deprecation fix)
    in_window = subset[
        (subset["t"] >= start.floor("h")) &
        (subset["t"] <= end.ceil("h"))
    ]

    if in_window.empty:
        return None

    temps_c = pd.to_numeric(in_window["temperature_2m"], errors="coerce").dropna().tolist()
    clouds  = pd.to_numeric(in_window["cloudcover"], errors="coerce").dropna().tolist()
    ws_ms   = pd.to_numeric(in_window["windspeed_10m"], errors="coerce").dropna().tolist()
    wd_deg  = pd.to_numeric(in_window["winddirection_10m"], errors="coerce").dropna().tolist()
    codes   = pd.to_numeric(in_window["weathercode"], errors="coerce").dropna().astype(int).tolist()

    if not temps_c:
        return None

    temp_c_avg = float(np.mean(temps_c))
    temp_f_avg = temp_c_avg * 9/5 + 32.0
    wind_mps_avg = float(np.mean(ws_ms)) if ws_ms else None
    wind_mph_avg = wind_mps_avg * 2.23694 if wind_mps_avg is not None else None
    wind_dir_avg = circular_mean_deg(wd_deg) if wd_deg else None
    condition = wx_condition_from(clouds, codes)

    return {
        "wx_temp_avg_f": round(temp_f_avg, 1),
        "wx_wind_speed_avg_mph": round(wind_mph_avg, 1) if wind_mph_avg is not None else None,
        "wx_wind_dir_avg_deg": round(wind_dir_avg, 0) if wind_dir_avg is not None else None,
        "wx_condition": condition,
    }, cache_df

def enrich_missing_weather(df_acts: pd.DataFrame, cache_df: pd.DataFrame):
    need_cols = ["wx_temp_avg_f","wx_wind_speed_avg_mph","wx_wind_dir_avg_deg","wx_condition"]
    # Identify rows missing any weather column
    missing_mask = pd.Series(False, index=df_acts.index)
    for c in need_cols:
        if c not in df_acts.columns:
            df_acts[c] = None
        missing_mask = missing_mask | df_acts[c].isna() | (df_acts[c] == "")
    todo = df_acts[missing_mask].copy()
    print(f"Weather: {len(todo)} activities missing weather → computing…")

    updated = 0
    for idx, row in todo.iterrows():
        res = compute_wx_for_activity(row, cache_df)
        if res is None:
            continue
        vals, cache_df = res
        for k, v in vals.items():
            df_acts.at[idx, k] = v
        updated += 1
        if updated % 10 == 0:
            print(f"  …updated {updated} activities")
        time.sleep(0.05)
    print(f"Weather: filled {updated} activities.")
    return df_acts, cache_df

# ---------------- Main ----------------
def main():
    sh = open_sheet()

    # Load existing activities (if any)
    df_existing = read_tab_df(sh, TAB_ACTIVITIES)
    if not df_existing.empty and "id" in df_existing.columns:
        df_existing["id"] = pd.to_numeric(df_existing["id"], errors="coerce")

    # Fetch fresh from Strava
    token = get_strava_access_token()
    gear_map = fetch_gear_map(token)
    acts = fetch_all_activities(token)
    df_incoming = normalize_activities(acts, gear_map)

    # Incremental merge
    if df_existing.empty:
        df_combined = df_incoming.copy()
        df_new = df_incoming.copy()
        print(f"Incremental: sheet empty → will write {len(df_combined)} activities.")
    else:
        df_combined, df_new = merge_incremental(df_existing, df_incoming)
        print(f"Incremental: {len(df_new)} new activities (total now {len(df_combined)}).")

    # Riders long (rebuilt from combined for consistency)
    df_riders = build_riders_long(df_combined)

    # Weather cache load/update + enrich weather for activities missing wx fields
    df_cache = load_weather_cache(sh)
    df_combined, df_cache = enrich_missing_weather(df_combined, df_cache)

    # Write tabs
    write_df_to_tab(sh, TAB_WX_CACHE, df_cache)
    write_df_to_tab(sh, TAB_RIDERS_LONG, df_riders)
    write_df_to_tab(sh, TAB_ACTIVITIES, df_combined)

    # Final summary
    titles = [ws.title for ws in sh.worksheets()]
    print("Worksheets present:", titles)

if __name__ == "__main__":
    main()
