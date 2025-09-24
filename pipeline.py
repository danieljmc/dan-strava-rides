import os, json, time
from datetime import datetime
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ------------- Config -------------
PER_PAGE = 200
SHEET_TAB_NAME = "activities_all"   # new tab with all summary fields
THROTTLE_S = 0.2                    # be polite between pages
ADD_GEAR_NAME = True                # map gear_id -> gear_name via /athlete

# ------------- Strava auth -------------
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

# ------------- Fetch activities (summary) -------------
def fetch_all_activities(access_token):
    all_rows = []
    page = 1
    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": PER_PAGE, "page": page},
            timeout=60,
        )
        if resp.status_code == 429:
            # rate-limited; wait a bit
            print("Rate limited; sleeping 60s...")
            time.sleep(60)
            continue
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        print(f"Fetched page {page} -> {len(batch)} activities (total {len(all_rows)}).")
        page += 1
        time.sleep(THROTTLE_S)
    return all_rows

# ------------- Optional: map gear_id -> gear_name -------------
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

# ------------- Flatten summary JSON to DataFrame -------------
def normalize_activities(acts, gear_map):
    # Flatten the JSON. meta_prefix keeps nested keys readable.
    df = pd.json_normalize(acts, sep=".")
    # Add human-friendly date/time columns
    for col in ("start_date_local", "start_date"):
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=False)
            df[col + "_fmt"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    # Add gear_name alongside gear_id
    if "gear_id" in df.columns:
        df["gear_name"] = df["gear_id"].map(lambda g: gear_map.get(g) if gear_map else None)
    # Convert lists/dicts to JSON strings so Sheets can hold them
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
            df[c] = df[c].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    # Sort newest first
    if "start_date_local" in df.columns:
        df = df.sort_values("start_date_local", ascending=False, kind="mergesort")
    elif "start_date" in df.columns:
        df = df.sort_values("start_date", ascending=False, kind="mergesort")
    return df

# ------------- Google Sheets I/O -------------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def write_dataframe_to_sheet(df):
    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    try:
        ws = sh.worksheet(SHEET_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB_NAME, rows=str(len(df) + 50), cols=str(max(26, len(df.columns) + 5)))
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"Wrote {len(df)} rows x {len(df.columns)} cols to tab '{SHEET_TAB_NAME}'.")

def main():
    token = get_strava_access_token()
    gear_map = fetch_gear_map(token)
    acts = fetch_all_activities(token)
    df = normalize_activities(acts, gear_map)
    write_dataframe_to_sheet(df)

if __name__ == "__main__":
    main()
