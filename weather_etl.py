# weather_etl.py — incremental weather ETL: only process missing activities & keep existing rows
import os, json, math, time, re
from datetime import datetime, timedelta, date
from typing import Tuple, Optional

import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

PRINT_PREFIX = "[WX]"
OM_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OM_ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/archive"
THROTTLE_S = 0.2
REQ_TIMEOUT_S = 90

TAB_ACTIVITIES = "activities_all"
TAB_WX_HOURLY  = "weather_hourly"
TAB_WX_BY_RIDE = "weather_by_ride"

# Controls (env)
WX_SINCE         = os.getenv("WX_SINCE")                 # e.g., "2025-06-01"
WX_MAX_RIDES     = int(os.getenv("WX_MAX_RIDES", "0"))   # 0 = no cap
WX_FLUSH_EVERY   = int(os.getenv("WX_FLUSH_EVERY", "25"))
WX_FORCE_REBUILD = os.getenv("WX_FORCE_REBUILD", "0") in ("1","true","True","YES","yes")

# ---------------- Google Sheets helpers ----------------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def read_tab_to_df(sh, title: str) -> pd.DataFrame:
    ws = sh.worksheet(title)
    try:
        data = ws.get_all_records(numeric_value_strategy="RAW")
    except TypeError:
        data = ws.get_all_records()
    return pd.DataFrame(data)

def _ensure_tab_with_headers(sh, title: str, headers: list[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="200", cols=str(max(26, len(headers)+5)))
    ws.clear()
    ws.update(values=[headers], range_name="A1")
    ws.resize(rows=200, cols=max(26, len(headers)+5))
    print(f"{PRINT_PREFIX} prepared tab '{title}' with headers.")

def write_df_to_tab(sh, title: str, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+50, 200)), cols=str(max(26, len(df.columns)+5)))
    ws.clear()
    if df.empty:
        headers = list(df.columns) if len(df.columns) > 0 else ["activity_id"]
        ws.update(values=[headers], range_name="A1")
        ws.resize(rows=50, cols=max(5, len(headers)))
        print(f"{PRINT_PREFIX} wrote headers only to '{title}'.")
        return
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"{PRINT_PREFIX} wrote {len(df)} rows x {len(df.columns)} cols to '{title}'.")

# ---------------- Weather / math utils ----------------
def c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0

def ms_to_mph(ms: float) -> float:
    return ms * 2.23694

def circular_mean_deg(deg_series: pd.Series) -> Optional[float]:
    vals = pd.to_numeric(deg_series, errors="coerce").dropna().values
    if len(vals) == 0:
        return None
    rad = np.deg2rad(vals)
    s = np.sin(rad).sum()
    c = np.cos(rad).sum()
    mean_rad = math.atan2(s, c)
    mean_deg = np.rad2deg(mean_rad)
    if mean_deg < 0:
        mean_deg += 360.0
    return float(mean_deg)

def parse_latlon(start_latlng_val) -> Optional[Tuple[float, float]]:
    """start_latlng is a JSON string like '[41.63,-71.17]'. Return (lat, lon) or None."""
    if start_latlng_val in (None, "", "[]", "null"):
        return None
    try:
        arr = start_latlng_val if isinstance(start_latlng_val, list) else json.loads(start_latlng_val)
        if not isinstance(arr, (list, tuple)) or len(arr) < 2:
            return None
        lat = float(arr[0]); lon = float(arr[1])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        return lat, lon
    except Exception:
        return None

# ---------------- Open-Meteo helpers ----------------
def is_past_range(start_iso: str, end_iso: str) -> bool:
    end_d = pd.to_datetime(end_iso).date()
    return end_d <= (date.today() - timedelta(days=1))

def om_base_url_for_range(start_iso: str, end_iso: str) -> str:
    return OM_ARCHIVE_URL if is_past_range(start_iso, end_iso) else OM_FORECAST_URL

def om_request(lat: float, lon: float, start_iso: str, end_iso: str, retries: int = 3) -> dict:
    base_url = om_base_url_for_range(start_iso, end_iso)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,cloudcover,windspeed_10m,winddirection_10m,weathercode",
        "start_date": start_iso,
        "end_date": end_iso,
        "timezone": "auto",
    }
    backoff = 2.0
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(base_url, params=params, timeout=REQ_TIMEOUT_S)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} from OM", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            if attempt == retries:
                raise
            sleep_s = backoff ** attempt
            print(f"{PRINT_PREFIX} OM retry {attempt}/{retries} after error {str(e)[:120]}; sleeping {sleep_s:.1f}s…")
            time.sleep(sleep_s)

# ---------------- Time helpers ----------------
_TZ_RE = re.compile(r"([A-Za-z]+/[A-Za-z_]+)$")

def extract_tz_name(tz_field: Optional[str]) -> Optional[str]:
    """'(GMT-05:00) America/New_York' -> 'America/New_York'."""
    if not tz_field or not isinstance(tz_field, str):
        return None
    m = _TZ_RE.search(tz_field.strip())
    return m.group(1) if m else None

def to_local_naive_from_sheet_value(ts_val, tz_field: Optional[str]) -> Optional[datetime]:
    """
    Convert 'start_date_local' to true local tz-naive using sheet timezone:
      - If value has 'Z' or offset -> treat as UTC, convert to tz_field, drop tz
      - Else -> parse as naive local
    """
    tzname = extract_tz_name(tz_field)
    if ts_val is None or (isinstance(ts_val, float) and np.isnan(ts_val)):
        return None
    s = str(ts_val)
    if "Z" in s or "+" in s or s.endswith("Z"):
        ts = pd.to_datetime(s, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        ts_local = ts.tz_convert(tzname) if tzname else ts
        return ts_local.tz_localize(None).to_pydatetime()
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()

def to_utc_naive(ts: datetime, tzname: Optional[str]) -> datetime:
    """Return UTC-naive for stable comparisons."""
    ts_pd = pd.Timestamp(ts)
    if ts_pd.tz is not None:
        return ts_pd.tz_convert("UTC").tz_localize(None).to_pydatetime()
    if tzname:
        try:
            ts_local = ts_pd.tz_localize(tzname, nonexistent="shift_forward", ambiguous="NaT")
            if pd.isna(ts_local):
                ts_local = ts_pd.tz_localize(tzname, nonexistent="shift_forward", ambiguous=True)
        except Exception:
            ts_local = ts_pd.tz_localize("UTC")
    else:
        ts_local = ts_pd.tz_localize("UTC")
    return ts_local.tz_convert("UTC").tz_localize(None).to_pydatetime()

# ---------------- Core ETL ----------------
def build_weather(sh):
    # Read activities
    df_acts = read_tab_to_df(sh, TAB_ACTIVITIES)
    if df_acts.empty:
        print(f"{PRINT_PREFIX} activities_all is empty; nothing to do.")
        return pd.DataFrame(), pd.DataFrame()

    needed = {"id", "start_date_local", "elapsed_time", "start_latlng", "timezone"}
    missing = [c for c in needed if c not in df_acts.columns]
    if missing:
        raise ValueError(f"Missing required columns in {TAB_ACTIVITIES}: {missing}")

    # Read existing weather tabs (for incremental)
    try:
        df_by_ride_existing = read_tab_to_df(sh, TAB_WX_BY_RIDE)
    except gspread.WorksheetNotFound:
        df_by_ride_existing = pd.DataFrame(columns=["activity_id"])
    try:
        df_hourly_existing = read_tab_to_df(sh, TAB_WX_HOURLY)
    except gspread.WorksheetNotFound:
        df_hourly_existing = pd.DataFrame(columns=["activity_id","time_utc"])

    # Normalize existing keys
    if "activity_id" in df_by_ride_existing.columns:
        existing_ids = set(pd.to_numeric(df_by_ride_existing["activity_id"], errors="coerce").dropna().astype(int).tolist())
    else:
        existing_ids = set()

    # Parse activities
    df = df_acts.copy()
    df["activity_id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["elapsed_time_s"] = pd.to_numeric(df["elapsed_time"], errors="coerce")
    df["tz_name"] = df["timezone"].apply(extract_tz_name)
    df["start_dt_local"] = [
        to_local_naive_from_sheet_value(ts_val, tz_field)
        for ts_val, tz_field in zip(df["start_date_local"], df["timezone"])
    ]
    latlons = df["start_latlng"].apply(parse_latlon)
    df["lat"] = latlons.apply(lambda t: t[0] if t else np.nan)
    df["lon"] = latlons.apply(lambda t: t[1] if t else np.nan)

    # Optional scope
    if WX_SINCE:
        since_dt = pd.to_datetime(WX_SINCE, errors="coerce")
        if pd.notna(since_dt):
            df = df[pd.to_datetime(df["start_dt_local"]) >= since_dt]

    # Eligibility
    elig = df[
        df["activity_id"].notna()
        & pd.notna(df["start_dt_local"])
        & df["elapsed_time_s"].notna()
        & df["lat"].notna()
        & df["lon"].notna()
    ].copy()

    # Incremental filter: drop already-processed ids unless forced
    if not WX_FORCE_REBUILD and existing_ids:
        before = len(elig)
        elig = elig[~elig["activity_id"].astype(int).isin(existing_ids)]
        print(f"{PRINT_PREFIX} skipping {before - len(elig)} already-processed activities (kept {len(elig)} new).")

    if WX_MAX_RIDES > 0:
        elig = elig.sort_values("start_dt_local", ascending=False).head(WX_MAX_RIDES)

    total = len(elig)
    if total == 0:
        print(f"{PRINT_PREFIX} no new activities to process in scope.")
        # Ensure tabs exist and keep existing content
        hourly_headers = ["activity_id","ride_start_local","ride_end_local","lat","lon",
                          "time_utc","temp_c","cloudcover_pct","windspeed_ms","windspeed_mph",
                          "winddir_deg","weathercode","temp_f"]
        agg_headers = ["activity_id","ride_start_local","ride_end_local","lat","lon",
                       "avg_temp_f","avg_cloudcover_pct","avg_windspeed_mph","circmean_winddir_deg","hours_sampled"]
        _ensure_tab_with_headers(sh, TAB_WX_HOURLY, hourly_headers)
        _ensure_tab_with_headers(sh, TAB_WX_BY_RIDE, agg_headers)
        # Write existing back (no-op if empty)
        if not df_hourly_existing.empty:
            write_df_to_tab(sh, TAB_WX_HOURLY, df_hourly_existing)
        if not df_by_ride_existing.empty:
            write_df_to_tab(sh, TAB_WX_BY_RIDE, df_by_ride_existing)
        return pd.DataFrame(), pd.DataFrame()

    # Prepare tabs up-front
    hourly_headers = ["activity_id","ride_start_local","ride_end_local","lat","lon",
                      "time_utc","temp_c","cloudcover_pct","windspeed_ms","windspeed_mph",
                      "winddir_deg","weathercode","temp_f"]
    agg_headers = ["activity_id","ride_start_local","ride_end_local","lat","lon",
                   "avg_temp_f","avg_cloudcover_pct","avg_windspeed_mph","circmean_winddir_deg","hours_sampled"]
    _ensure_tab_with_headers(sh, TAB_WX_HOURLY, hourly_headers)
    _ensure_tab_with_headers(sh, TAB_WX_BY_RIDE, agg_headers)

    # Accumulators (new rows only)
    hourly_rows_new = []
    agg_rows_new = []

    for i, (_, row) in enumerate(elig.iterrows(), start=1):
        aid = int(row["activity_id"])
        tzname = row["tz_name"] or "UTC"
        start_dt_local: datetime = row["start_dt_local"]
        end_dt_local: datetime = start_dt_local + timedelta(seconds=float(row["elapsed_time_s"]))
        lat, lon = float(row["lat"]), float(row["lon"])

        # OM date span (local)
        start_day = start_dt_local.date()
        end_day   = end_dt_local.date()
        om_start = start_day.isoformat()
        om_end   = end_day.isoformat()

        try:
            data = om_request(lat, lon, om_start, om_end)
        except Exception as e:
            print(f"{PRINT_PREFIX} OM error for activity {aid}: {str(e)[:140]}")
            continue

        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        temp_c = hourly.get("temperature_2m", [])
        clouds = hourly.get("cloudcover", [])
        wind_s = hourly.get("windspeed_10m", [])
        wind_d = hourly.get("winddirection_10m", [])
        codes  = hourly.get("weathercode", [])

        times_utc_naive = pd.Series(pd.to_datetime(times, utc=True).tz_convert("UTC").tz_localize(None))
        start_utc_naive = to_utc_naive(start_dt_local, tzname)
        end_utc_naive   = to_utc_naive(end_dt_local,   tzname)

        df_h = pd.DataFrame({
            "time_utc": times_utc_naive,
            "temp_c": pd.to_numeric(temp_c, errors="coerce"),
            "cloudcover_pct": pd.to_numeric(clouds, errors="coerce"),
            "windspeed_ms": pd.to_numeric(wind_s, errors="coerce"),
            "winddir_deg": pd.to_numeric(wind_d, errors="coerce"),
            "weathercode": pd.to_numeric(codes, errors="coerce")
        }).dropna(subset=["time_utc"])

        start_floor_utc = pd.Timestamp(start_utc_naive).replace(minute=0, second=0, microsecond=0).to_pydatetime()
        end_floor_utc   = pd.Timestamp(end_utc_naive).replace(minute=0, second=0, microsecond=0).to_pydatetime()
        mask = (df_h["time_utc"] >= start_floor_utc) & (df_h["time_utc"] <= end_floor_utc)
        df_h = df_h.loc[mask].copy()

        if df_h.empty:
            # Grab the first hour at/after start if short ride doesn’t span a whole hour
            idxs = np.where(times_utc_naive.values >= start_floor_utc)[0]
            if len(idxs) > 0:
                j = int(idxs[0])
                df_h = pd.DataFrame({
                    "time_utc": [times_utc_naive.iloc[j]],
                    "temp_c": [pd.to_numeric(temp_c[j], errors="coerce")],
                    "cloudcover_pct": [pd.to_numeric(clouds[j], errors="coerce")],
                    "windspeed_ms": [pd.to_numeric(wind_s[j], errors="coerce")],
                    "winddir_deg": [pd.to_numeric(wind_d[j], errors="coerce")],
                    "weathercode": [pd.to_numeric(codes[j], errors="coerce")]
                })

        if df_h.empty:
            print(f"{PRINT_PREFIX} no overlap for activity {aid} ({start_dt_local} → {end_dt_local}).")
            continue

        # Per-hour conversions
        df_h["temp_f"] = df_h["temp_c"].apply(lambda x: c_to_f(x) if pd.notna(x) else np.nan)
        df_h["windspeed_mph"] = df_h["windspeed_ms"].apply(lambda x: ms_to_mph(x) if pd.notna(x) else np.nan)

        # Add identifiers
        df_h.insert(0, "activity_id", aid)
        df_h.insert(1, "ride_start_local", start_dt_local)
        df_h.insert(2, "ride_end_local", end_dt_local)
        df_h.insert(3, "lat", lat)
        df_h.insert(4, "lon", lon)
        hourly_rows_new.append(df_h)

        # Aggregations (ride-level)
        temp_f_avg = float(df_h["temp_f"].mean()) if not df_h["temp_f"].dropna().empty else None
        clouds_avg = float(df_h["cloudcover_pct"].mean()) if not df_h["cloudcover_pct"].dropna().empty else None
        wind_s_avg = float(df_h["windspeed_mph"].mean()) if not df_h["windspeed_mph"].dropna().empty else None
        wind_d_cmn = circular_mean_deg(df_h["winddir_deg"])
        agg_rows_new.append({
            "activity_id": aid,
            "ride_start_local": start_dt_local,
            "ride_end_local": end_dt_local,
            "lat": lat, "lon": lon,
            "avg_temp_f": temp_f_avg,
            "avg_cloudcover_pct": clouds_avg,
            "avg_windspeed_mph": wind_s_avg,
            "circmean_winddir_deg": wind_d_cmn,
            "hours_sampled": int(len(df_h))
        })

        # Partial flush (combine existing + new-so-far, dedupe, then write)
        if i % WX_FLUSH_EVERY == 0:
            df_hourly_new_so_far = pd.concat(hourly_rows_new, ignore_index=True) if hourly_rows_new else pd.DataFrame(columns=["activity_id","time_utc"])
            df_by_ride_new_so_far = pd.DataFrame(agg_rows_new) if agg_rows_new else pd.DataFrame(columns=["activity_id"])

            # Combine & dedupe
            hourly_combined = pd.concat([df_hourly_existing, df_hourly_new_so_far], ignore_index=True)
            if not hourly_combined.empty:
                hourly_combined["activity_id"] = pd.to_numeric(hourly_combined["activity_id"], errors="coerce").astype("Int64")
                # Use activity_id + time_utc as unique key
                if "time_utc" in hourly_combined.columns:
                    hourly_combined = hourly_combined.drop_duplicates(subset=["activity_id","time_utc"], keep="last")

            byride_combined = pd.concat([df_by_ride_existing, df_by_ride_new_so_far], ignore_index=True)
            if not byride_combined.empty:
                byride_combined["activity_id"] = pd.to_numeric(byride_combined["activity_id"], errors="coerce").astype("Int64")
                byride_combined = byride_combined.drop_duplicates(subset=["activity_id"], keep="last")

            write_df_to_tab(sh, TAB_WX_HOURLY, hourly_combined)
            write_df_to_tab(sh, TAB_WX_BY_RIDE, byride_combined)
            print(f"{PRINT_PREFIX} progress: flushed {i}/{total} new rides.")

        print(f"{PRINT_PREFIX} processed {i}/{total} (id={aid}).")
        time.sleep(THROTTLE_S)

    # Final combine & write
    df_hourly_new = pd.concat(hourly_rows_new, ignore_index=True) if hourly_rows_new else pd.DataFrame(columns=["activity_id","time_utc"])
    df_by_ride_new = pd.DataFrame(agg_rows_new) if agg_rows_new else pd.DataFrame(columns=["activity_id"])

    hourly_final = pd.concat([df_hourly_existing, df_hourly_new], ignore_index=True)
    if not hourly_final.empty:
        hourly_final["activity_id"] = pd.to_numeric(hourly_final["activity_id"], errors="coerce").astype("Int64")
        if "time_utc" in hourly_final.columns:
            hourly_final = hourly_final.drop_duplicates(subset=["activity_id","time_utc"], keep="last")

    byride_final = pd.concat([df_by_ride_existing, df_by_ride_new], ignore_index=True)
    if not byride_final.empty:
        byride_final["activity_id"] = pd.to_numeric(byride_final["activity_id"], errors="coerce").astype("Int64")
        byride_final = byride_final.drop_duplicates(subset=["activity_id"], keep="last")

    return hourly_final, byride_final

def main():
    print(f"{PRINT_PREFIX} START weather ETL (incremental)")
    if WX_SINCE: print(f"{PRINT_PREFIX} WX_SINCE={WX_SINCE}")
    if WX_MAX_RIDES: print(f"{PRINT_PREFIX} WX_MAX_RIDES={WX_MAX_RIDES}")
    print(f"{PRINT_PREFIX} WX_FLUSH_EVERY={WX_FLUSH_EVERY}")
    if WX_FORCE_REBUILD: print(f"{PRINT_PREFIX} WX_FORCE_REBUILD=1 (ignoring existing)")

    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    df_hourly_final, df_by_ride_final = build_weather(sh)

    # Write final state
    write_df_to_tab(sh, TAB_WX_HOURLY, df_hourly_final)
    write_df_to_tab(sh, TAB_WX_BY_RIDE, df_by_ride_final)

    titles = [ws.title for ws in sh.worksheets()]
    print(f"{PRINT_PREFIX} worksheets present: {titles}")
    print(f"{PRINT_PREFIX} DONE")

if __name__ == "__main__":
    main()
