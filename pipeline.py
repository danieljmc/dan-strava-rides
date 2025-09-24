# pipeline.py  (riders v3)
import os, json, time, re
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

print("PIPELINE VERSION: riders v3")

# ---------------- Config ----------------
PER_PAGE = 200
TAB_ACTIVITIES = "activities_all"   # wide table: all summary fields + parsed riders
TAB_RIDERS_LONG = "riders_long"     # long table: (activity_id, rider_name)
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

# ---------------- Fetch activities (summary) ----------------
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

STOPWORDS = {
    "solo","loop","ride","walk","hike","skate","skating","biking","cycling","trainer","activity",
    "morning","afternoon","evening","lunch","first","return","reverse","leisurely","slow",
    "rd","road","street","blvd","park","path","trail","drive","point","beach","lake","falls","notch",
    "green","bridge","bridges","river","coast","campground","maze","summit",
    "north","south","east","west",
    "boys","family","home","dad",
    "ebbp","nh","ri","fl",
    "newport","providence","sakonnet","sakonnett","acoaxet","bulgamarsh","horseneck",
    "padenarem","padanaram","dartmouth","westport","adamsville","sepowet","seapowet","blackstone",
    "bristol","portsmouth","lincoln","waterville","valley","kancamagus","conway","taunton",
}

KEEP_PHRASES = {"the boys"}

def strip_parens(t: str) -> str:
    t = t.strip()
    if t.startswith("(") and t.endswith(")"):
        return t[1:-1].strip()
    return t

def is_name_token(tok: str) -> bool:
    if not tok:
        return False
    lt = tok.lower()
    if lt in KEEP_PHRASES: return True
    if lt in STOPWORDS:    return False
    if any(ch.isdigit() for ch in tok): return False
    if tok.endswith("'s") and lt != "o's": return False
    if len(tok) >= 2 and tok.isupper():   return False
    return tok[0].isupper()

def split_by_commas_and_and(s: str) -> list[str]:
    s = s.replace("&", " and ")
    s = re.sub(r"\s+and\s+", ",", s, flags=re.IGNORECASE)
    return [p.strip() for p in s.split(",") if p.strip()]

def tokenize_names(segment: str) -> list[str]:
    out = []
    for part in split_by_commas_and_and(segment):
        core = strip_parens(part)
        if core.lower() in KEEP_PHRASES:
            out.append(core.title()); continue
        for w in core.split():
            w = w.strip()
            if is_name_token(w):
                out.append(w.title())
    # de-dup preserve order
    seen, unique = set(), []
    for t in out:
        if t not in seen:
            unique.append(t); seen.add(t)
    return unique

def extract_riders_from_name(activity_name: str) -> list[str]:
    if not activity_name:
        return []
    m = WITH_PATTERN.search(activity_name)
    if m:
        after = activity_name[m.end():]
        names = tokenize_names(after)
        if names:
            return names
    # fallback: scan whole title
    return tokenize_names(activity_name)

# ---------------- Flatten + enrich ----------------
def normalize_activities(acts, gear_map):
    df = pd.json_normalize(acts, sep=".")
    # datetime
    for col in ("start_date_local", "start_date"):
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce")
            df[col + "_fmt"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    # gear
    if "gear_id" in df.columns:
        df["gear_name"] = df["gear_id"].map(lambda g: gear_map.get(g) if gear_map else None)
    # riders
    if "name" in df.columns and "id" in df.columns:
        print("normalize_activities: 'name' and 'id' columns present.")
        riders_lists = df["name"].apply(extract_riders_from_name)
        df["other_riders"] = riders_lists.apply(lambda lst: ", ".join(lst) if lst else "")
        df["other_riders_count"] = riders_lists.apply(len)
        df["other_riders_json"] = riders_lists.apply(lambda lst: json.dumps(lst) if lst else "[]")
        total_mentions = int(df["other_riders_count"].sum())
        activities_with_any = int((df["other_riders_count"] > 0).sum())
        print(f"Rider parsing: {activities_with_any} activities contain riders; {total_mentions} rider names total.")
        sample = df.loc[df["other_riders_count"] > 0, ["name","other_riders"]].head(5)
        if not sample.empty:
            print("Sample parsed riders:\n", sample.to_string(index=False))
    else:
        print("normalize_activities: MISSING 'name' or 'id' — rider parsing skipped.")
    # lists/dicts -> JSON strings
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
            df[c] = df[c].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    # sort
    sort_col = "start_date_local" if "start_date_local" in df.columns else ("start_date" if "start_date" in df.columns else None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False, kind="mergesort")
    return df

def build_riders_long(df_activities: pd.DataFrame) -> pd.DataFrame:
    cols = ["activity_id", "rider_name"]
    if "id" not in df_activities.columns or "other_riders" not in df_activities.columns:
        print("riders_long: missing 'id' or 'other_riders'; returning empty.")
        return pd.DataFrame(columns=cols)
    rows = []
    for act_id, riders in zip(df_activities["id"], df_activities["other_riders"]):
        if not riders:
            continue
        for rider in [r.strip() for r in riders.split(",") if r.strip()]:
            rows.append({"activity_id": int(act_id), "rider_name": rider})
    df = pd.DataFrame(rows, columns=cols)
    print(f"riders_long rows: {len(df)}")
    return df

# ---------------- Google Sheets I/O ----------------
def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    # Include Drive scope (helps in some environments / shared drives)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

def write_df_to_tab(sh, title: str, df: pd.DataFrame):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+50, 200)), cols=str(max(26, len(df.columns)+5)))
    ws.clear()
    if df.empty:
        headers = list(df.columns) if len(df.columns) > 0 else ["activity_id","rider_name"]
        ws.update("A1", [headers])
        ws.resize(rows=50, cols=max(5, len(headers)))
        print(f"Wrote 0 rows to tab '{title}' (headers only).")
        return
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
    print(f"Wrote {len(df)} rows x {len(df.columns)} cols to tab '{title}'.")

def write_dataframes_to_sheet(df_acts: pd.DataFrame, df_riders: pd.DataFrame):
    gc = gspread_client_from_secret()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    # Write riders FIRST so you can see its log line even if activities succeeds
    write_df_to_tab(sh, TAB_RIDERS_LONG, df_riders)
    write_df_to_tab(sh, TAB_ACTIVITIES, df_acts)

    # Final debug: list tabs present
    titles = [ws.title for ws in sh.worksheets()]
    print("Worksheets now present:", titles)

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
