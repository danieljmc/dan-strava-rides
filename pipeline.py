import os, json, time, re
from datetime import datetime
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ---------------- Config ----------------
PER_PAGE = 200
TAB_ACTIVITIES = "activities_all"   # wide
TAB_RIDERS_LONG = "riders_long"     # long (activity_id, rider_name)
THROTTLE_S = 0.2
ADD_GEAR_NAME = True

# ---------------- Strava auth ----------------
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

# ---------------- Fetch activities ----------------
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

# ---------------- Gear name map ----------------
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

# ---------------- Riders parsing ----------------
WITH_PATTERN = re.compile(r"\bwith\b", flags=re.IGNORECASE)

def strip_parens(text: str) -> str:
    text = text.strip()
    if text.startswith("(") and text.endswith(")"):
        return text[1:-1].strip()   # keep inside as one token (e.g., "the boys")
    return text

def split_riders(after_with: str) -> list[str]:
    """
    Splits riders after 'with' into *individual names*:
      - "Rebecca Russell and Thomas (the boys)" -> ["Rebecca","Russell","Thomas","the boys"]
      - "Thomas and Al" -> ["Thomas","Al"]
      - "Al, Russell, Matt, and Dan" -> ["Al","Russell","Matt","Dan"]
      - "Al & Dan" -> ["Al","Dan"]
    """
    if not after_with:
        return []

    s = after_with
    s = s.replace("&", " and ")
    s = re.sub(r"\s+and\s+", ",", s, flags=re.IGNORECASE)  # normalize 'and' to commas
    parts = [p.strip() for p in s.split(",") if p.strip()]

    tokens: list[str] = []
    for p in parts:
        core = strip_parens(p)  # remove outer parens but keep inner text as one piece
        # Now split names by whitespace into individual tokens
        # e.g. "Rebecca Russell" -> ["Rebecca","Russell"]
        #     "the boys" (from parens) stays as one token because we already stripped parens above
        if core.lower() == "the boys":
            tokens.append(core.title())  # keep phrase as one token
        else:
            tokens.extend([w for w in core.split() if w])

    # Title-case simple tokens (won't perfectly handle every surname, but matches your request)
    tokens = [t if t.isupper() else t.title() for t in tokens]

    # De-dup while preserving order
    seen, out = set(), []
    for t in tokens:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def extract_riders_from_name(activity_name: str) -> list[str]:
    if not activity_name:
        return []
    m = WITH_PATTERN.search(activity_name)
    if not m:
        return []
    return split_riders(activity_name[m.end():])


# ---------------- Flatten + enrich ----------------
def normalize_activities(acts, gear_map):
    df = pd.json_normalize(acts, sep=".")
    # Add pretty datetime strings
    for col in ("start_date_local", "start_date"):
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce")
            df[col + "_fmt"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    # Gear name
    if "gear_id" in df.columns:
        df["gear_name"] = df["gear_id"].map(lambda g: gear_map.get(g) if gear_map else None)
    # Riders (from name)
    if "name" in df.columns and "id" in df.columns:
        riders_lists = df["name"].apply(extract_riders_from_name)
        df["other_riders"] = riders_lists.apply(lambda lst: ", ".join(lst) if lst else "")
        df["other_riders_count"] = riders_lists.apply(len)
        # also keep JSON list if you want
        df["other_riders_json"] = riders_lists.apply(lambda lst: json.dumps(lst) if lst else "[]")
    # Convert nested lists/dicts to JSON strings so Sheets can hold them
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
            df[c] = df[c].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    # Sort newest first
    sort_col = "start_date_local" if "start_date_local" in df.columns else ("start_date" if "start_date" in df.columns else None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False, kind="mergesort")
    return df

def build_riders_long(df_activities: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if "id" not in df_activities.columns or "other_riders" not in df_activities.columns:
        return pd.DataFrame(columns=["activity_id","rider_name"])
    for act_id, riders in zip(df_activities["id"], df_activities["other_riders"]):
        if not riders:
            continue
        for rider in [r.strip() for r in riders.split(",") if r.strip()]:
            rows.append({"activity_id": act_id, "rider_name": rider})
    return pd.DataFrame(rows)

# ---------------- Google Sheets I/O ----------------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def write_df_to_tab(sh, title: str, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+50, 200)), cols=str(max(26, len(df.columns)+5)))
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"Wrote {len(df)} rows x {len(df.columns)} cols to tab '{title}'.")

def write_dataframes_to_sheet(df_acts: pd.DataFrame, df_riders: pd.DataFrame):
    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    write_df_to_tab(sh, TAB_ACTIVITIES, df_acts)
    write_df_to_tab(sh, TAB_RIDERS_LONG, df_riders)

# ---------------- Main ----------------
def main():
    token = get_strava_access_token()
    gear_map = fetch_gear_map(token)
    acts = fetch_all_activities(token)
    df_acts = normalize_activities(acts, gear_map)
    df_riders = build_riders_long(df_acts)
    write_dataframes_to_sheet(df_acts, df_riders)

if __name__ == "__main__":
    main()
