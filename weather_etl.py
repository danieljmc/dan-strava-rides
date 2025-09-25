# weather_etl.py — pull hourly weather for each ride window and write to Google Sheets
import os, json, math, time
from datetime import datetime, timedelta
from typing import Tuple, Optional

import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

PRINT_PREFIX = "[WX]"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
THROTTLE_S = 0.2  # be kind to OM

TAB_ACTIVITIES = "activities_all"
TAB_WX_HOURLY  = "weather_hourly"
TAB_WX_BY_RIDE = "weather_by_ride"

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
    # Back-compatible: older gspread may lack numeric_value_strategy
    try:
        data = ws.get_all_records(numeric_value_strategy="RAW")
    except TypeError:
        data = ws.get_all_records()
    return pd.DataFrame(data)

def write_df_to_tab(sh, title: str, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+50, 200)), cols=str(max(26, len(df.columns)+5)))
    ws.clear()
    if df.empty:
        headers = list(df.columns) if len(df.columns) > 0 else ["activity_id"]
        ws.update("A1", [headers])
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
    """start_latlng is stored as a JSON string like '[41.63,-71.17]'. Return (lat, lon) or None."""
    if start_latlng_val in (None, "", "[]", "null"):
        return None
    try:
        if isinstance(start_latlng_val, list):
            arr = start_latlng_val
        else:
            arr = json.loads(start_latlng_val)
        if not isinstance(arr, (list, tuple)) or len(arr) < 2:
            return None
        lat = float(arr[0]); lon = float(arr[1])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        return lat, lon
    except Exception:
        return None

def fetch_open_meteo_hourly(lat: float, lon: float, start_date_iso: str, end_date_iso: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,cloudcover,windspeed_10m,winddirection_10m,weathercode",
        "start_date": start_date_iso,
        "end_date": end_date_iso,
        "timezone": "auto",  # OM returns tz-aware times + a 'timezone' field
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    if r.status_code == 429:
        print(f"{PRINT_PREFIX} rate limited; sleeping 60s…")
        time.sleep(60)
        r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def to_utc_naive(ts: datetime, tzname: Optional[str]) -> datetime:
    """
    Convert a timestamp to UTC-naive:
      - if ts is tz-aware: tz_convert('UTC') and drop tz
      - if ts is tz-naive: tz_localize(tzname) -> tz_convert('UTC') -> drop tz
    """
    ts_pd = pd.Timestamp(ts)
    if ts_pd.tz is not None:
        ts_utc = ts_pd.tz_convert("UTC")
        return ts_utc.tz_localize(None).to_pydatetime()
    # tz-naive path
    if tzname:
        try:
            ts_local = ts_pd.tz_localize(tzname, nonexistent="shift_forward", ambiguous="NaT")
            if pd.isna(ts_local):
                ts_local = ts_pd.tz_localize(tzname, nonexistent="shift_forward", ambiguous=True)
        except Exception:
            # last resort: assume it's already UTC wall clock
            ts_local = ts_pd.tz_localize("UTC")
    else:
        ts_local = ts_pd.tz_localize("UTC")
    return ts_local.tz_convert("UTC").tz_localize(None).to_pydatetime()

# ---------------- Core ETL ----------------
def build_weather(sh):
    df_acts = read_tab_to_df(sh, TAB_ACTIVITIES)
    if df_acts.empty:
        print(f"{PRINT_PREFIX} activities_all is empty; nothing to do.")
        return pd.DataFrame(), pd.DataFrame()

    needed = {"id", "start_date_local", "elapsed_time", "start_latlng"}
    missing = [c for c in needed if c not in df_acts.columns]
    if missing:
        raise ValueError(f"Missing required columns in {TAB_ACTIVITIES}: {missing}")

    df = df_acts.copy()
    df["activity_id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["elapsed_time_s"] = pd.to_numeric(df["elapsed_time"], errors="coerce")
    # start_date_local may be tz-naive or tz-aware depending on how the sheet stored it
    df["start_dt_local"] = pd.to_datetime(df["start_date_local"], errors="coerce")

    latlons = df["start_latlng"].apply(parse_latlon)
    df["lat"] = latlons.apply(lambda t: t[0] if t else np.nan)
    df["lon"] = latlons.apply(lambda t: t[1] if t else np.nan)

    elig = df[
        df["activity_id"].notna()
        & df["start_dt_local"].notna()
        & df["elapsed_time_s"].notna()
        & df["lat"].notna()
        & df["lon"].notna()
    ].copy()

    if elig.empty:
        print(f"{PRINT_PREFIX} no activities with usable start/elapsed/latlon.")
        return pd.DataFrame(), pd.DataFrame()

    hourly_rows = []
    agg_rows = []

    for _, row in elig.iterrows():
        aid = int(row["activity_id"])
        start_dt_local: datetime = row["start_dt_local"].to_pydatetime()
        end_dt_local: datetime = start_dt_local + timedelta(seconds=float(row["elapsed_time_s"]))
        lat, lon = float(row["lat"]), float(row["lon"])

        # Span the involved local dates for the OM call
        start_day = start_dt_local.date()
        end_day   = end_dt_local.date()
        om_start = start_day.isoformat()
        om_end   = end_day.isoformat()

        try:
            data = fetch_open_meteo_hourly(lat, lon, om_start, om_end)
        except Exception as e:
            print(f"{PRINT_PREFIX} OM error for activity {aid}: {e}")
            continue

        # OM metadata timezone, e.g. "America/New_York"
        om_tz = data.get("timezone") or "UTC"

        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        temp_c = hourly.get("temperature_2m", [])
        clouds = hourly.get("cloudcover", [])
        wind_s = hourly.get("windspeed_10m", [])
        wind_d = hourly.get("winddirection_10m", [])
        codes  = hourly.get("weathercode", [])

        # Parse OM times as tz-aware → UTC → tz-naive (Series so we can use .tz_* accessors)
        times_utc_naive = pd.Series(pd.to_datetime(times, utc=True).tz_convert("UTC").tz_localize(None))

        # Convert ride window to UTC-naive
        start_utc_naive = to_utc_naive(start_dt_local, om_tz)
        end_utc_naive   = to_utc_naive(end_dt_local,   om_tz)

        # Build per-hour DataFrame in UTC-naive clock for comparisons
        df_h = pd.DataFrame({
            "time_utc": times_utc_naive,
            "temp_c": pd.to_numeric(temp_c, errors="coerce"),
            "cloudcover_pct": pd.to_numeric(clouds, errors="coerce"),
            "windspeed_ms": pd.to_numeric(wind_s, errors="coerce"),
            "winddir_deg": pd.to_numeric(wind_d, errors="coerce"),
            "weathercode": pd.to_numeric(codes, errors="coerce")
        }).dropna(subset=["time_utc"])

        # Restrict to hours intersecting the ride [start, end] (floored to hour)
        start_floor_utc = pd.Timestamp(start_utc_naive).replace(minute=0, second=0, microsecond=0).to_pydatetime()
        end_floor_utc   = pd.Timestamp(end_utc_naive).replace(minute=0, second=0, microsecond=0).to_pydatetime()
        mask = (df_h["time_utc"] >= start_floor_utc) & (df_h["time_utc"] <= end_floor_utc)
        df_h = df_h.loc[mask].copy()

        if df_h.empty:
            # If nothing intersects (very short rides), take the first hour at/after start
            nearest_mask = (times_utc_naive >= start_floor_utc)
            if nearest_mask.any():
                idx = list(nearest_mask).index(True)
                df_h = pd.DataFrame({
                    "time_utc": [times_utc_naive.iloc[idx]],
                    "temp_c": [pd.to_numeric(temp_c[idx], errors="coerce")],
                    "cloudcover_pct": [pd.to_numeric(clouds[idx], errors="coerce")],
                    "windspeed_ms": [pd.to_numeric(wind_s[idx], errors="coerce")],
                    "winddir_deg": [pd.to_numeric(wind_d[idx], errors="coerce")],
                    "weathercode": [pd.to_numeric(codes[idx], errors="coerce")]
                })

        if df_h.empty:
            print(f"{PRINT_PREFIX} no hourly overlap found for activity {aid} ({start_dt_local} to {end_dt_local}).")
            continue

        # Per-hour conversions
        df_h["temp_f"] = df_h["temp_c"].apply(lambda x: c_to_f(x) if pd.notna(x) else np.nan)
        df_h["windspeed_mph"] = df_h["windspeed_ms"].apply(lambda x: ms_to_mph(x) if pd.notna(x) else np.nan)

        # Add identifiers (also keep original local start/end for reference)
        df_h.insert(0, "activity_id", aid)
        df_h.insert(1, "ride_start_local", start_dt_local)
        df_h.insert(2, "ride_end_local", end_dt_local)
        df_h.insert(3, "lat", lat)
        df_h.insert(4, "lon", lon)
        # time_utc already present

        hourly_rows.append(df_h)

        # Aggregations for the ride window
        temp_f_avg = float(df_h["temp_f"].mean()) if not df_h["temp_f"].dropna().empty else None
        clouds_avg = float(df_h["cloudcover_pct"].mean()) if not df_h["cloudcover_pct"].dropna().empty else None
        wind_s_avg = float(df_h["windspeed_mph"].mean()) if not df_h["windspeed_mph"].dropna().empty else None
        wind_d_cmn = circular_mean_deg(df_h["winddir_deg"])

        agg_rows.append({
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

        time.sleep(THROTTLE_S)

    df_hourly = pd.concat(hourly_rows, ignore_index=True) if hourly_rows else pd.DataFrame(
        columns=["activity_id","ride_start_local","ride_end_local","lat","lon",
                 "time_utc","temp_c","cloudcover_pct","windspeed_ms","windspeed_mph",
                 "winddir_deg","weathercode","temp_f"]
    )

    df_by_ride = pd.DataFrame(agg_rows, columns=[
        "activity_id","ride_start_local","ride_end_local","lat","lon",
        "avg_temp_f","avg_cloudcover_pct","avg_windspeed_mph","circmean_winddir_deg","hours_sampled"
    ])

    return df_hourly, df_by_ride

def main():
    print(f"{PRINT_PREFIX} START weather ETL")
    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    df_hourly, df_by_ride = build_weather(sh)

    # Write tabs
    write_df_to_tab(sh, TAB_WX_HOURLY, df_hourly)
    write_df_to_tab(sh, TAB_WX_BY_RIDE, df_by_ride)

    # Final debug: list tabs present
    titles = [ws.title for ws in sh.worksheets()]
    print(f"{PRINT_PREFIX} worksheets present: {titles}")
    print(f"{PRINT_PREFIX} DONE")

if __name__ == "__main__":
    main()
