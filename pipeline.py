# pipeline_incremental.py  (riders v7 – incremental)
import os, json, time, re, math
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from datetime import datetime, timezone, timedelta

print("=== PIPELINE START ===")
print("PIPELINE VERSION: riders v7 (incremental)")

# ---------------- Config ----------------
PER_PAGE = 200  # Strava max per page for /athlete/activities
TAB_ACTIVITIES = "activities_all"   # wide table: all summary fields + parsed riders
TAB_RIDERS_LONG = "riders_long"     # long table: (activity_id, rider_name)
THROTTLE_S = 0.2
ADD_GEAR_NAME = True
INCLUDE_SOLO_PLACEHOLDER = True
AFTER_SAFETY_BUFFER_S = 3600  # pull an extra hour just in case of timezone/clock drift

# ---------------- Auth ----------------
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

def gspread_client_from_secret():
    svc_info = json.loads(os.environ["GCP_SVC_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
    return gspread.authorize(creds)

# ---------------- Google Sheets helpers ----------------
def open_sheet():
    gc = gspread_client_from_secret()
    return gc.open_by_key(os.environ["SHEET_ID"])

def read_existing_activities_df(sh) -> pd.DataFrame:
    try:
        ws = sh.worksheet(TAB_ACTIVITIES)
    except gspread.WorksheetNotFound:
        print(f"Tab '{TAB_ACTIVITIES}' not found — treating this as first run (no prior data).")
        return pd.DataFrame()

    # Pull the whole tab as a DataFrame; keep default dtype inference
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    # Drop empty trailing rows/cols if present
    df = df.dropna(how="all").reset_index(drop=True)
    # Attempt to coerce id to int
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").dropna().astype("int64")
    return df

def latest_start_dt_utc(existing_df: pd.DataFrame) -> datetime | None:
    """
    Find the latest start timestamp from existing sheet data.
    Prefer 'start_date' (UTC from Strava). Fallback to 'start_date_local_fmt' if needed.
    """
    if existing_df.empty:
        return None

    # Try UTC first (native Strava field if present in prior saved columns)
    for col in ["start_date", "start_date_fmt"]:
        if col in existing_df.columns:
            ts = pd.to_datetime(existing_df[col], errors="coerce", utc=True)
            if ts.notna().any():
                latest = ts.max()
                if pd.notna(latest):
                    return latest.to_pydatetime()

    # Fallback to local (treat as local-naive; convert to UTC conservatively)
    for col in ["start_date_local", "start_date_local_fmt"]:
        if col in existing_df.columns:
            ts = pd.to_datetime(existing_df[col], errors="coerce")
            if ts.notna().any():
                # Assume local timestamps were local; we can't know tz, so use them directly.
                latest_local = ts.max()
                if pd.notna(latest_local):
                    # Treat as naive and subtract nothing—use safety buffer later.
                    return latest_local.tz_localize(None).replace(tzinfo=timezone.utc)
    return None

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
    print(f"Wrote {len(df)} rows × {len(df.columns)} cols to '{title}'.")

# ---------------- Gear ----------------
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
    "family","boys","dad","mom","wife","husband","kids","scouts","troop","group","team","party","committee","club",
    "home","washington","tiverton","fall","river","sepowet","boy","scout","hill","buck","secondary",
    "solo","loop","ride","walk","hike","skate","skating","biking","cycling","trainer","activity",
    "morning","afternoon","evening","lunch","first","return","reverse","leisurely","slow",
    "rd","road","street","blvd","park","path","trail","drive","point","beach","lake","falls","notch",
    "green","bridge","bridges","river","coast","campground","maze","summit","north","south","east","west",
    "ebbp","nh","ri","fl","newport","providence","sakonnet","sakonnett","acoaxet","bulgamarsh","horseneck",
    "padenarem","padanaram","dartmouth","westport","adamsville","seapowet","blackstone","bristol",
    "portsmouth","lincoln","waterville","valley","kancamagus","conway","taunton",
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
    if not tok: return False
    if any(ch.isdigit() for ch in tok): return False
    lt = tok.lower()
    if lt in STOPWORDS: return False
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
            if w and is_name_token(w):
                out.append(w.title())
    # de-dup, preserve order
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

# ---------------- Fetch (incremental) ----------------
def fetch_activities_after(access_token: str, after_epoch: int) -> list[dict]:
    """Fetch activities strictly AFTER the given epoch seconds."""
    all_rows, page = [], 1
    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": PER_PAGE, "page": page, "after": after_epoch},
            timeout=60,
        )
        if resp.status_code == 429:
            print("Rate limited; sleeping 60s..."); time.sleep(60); continue
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        print(f"[after={after_epoch}] page {page} -> {len(batch)} new activities (running total {len(all_rows)}).")
        page += 1
        time.sleep(THROTTLE_S)
    return all_rows

def fetch_all_activities(access_token: str) -> list[dict]:
    """Full backfill helper (first run / no sheet)."""
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
        if not batch:
            break
        all_rows.extend(batch)
        print(f"[full] page {page} -> {len(batch)} activities (running total {len(all_rows)}).")
        page += 1
        time.sleep(THROTTLE_S)
    return all_rows

# ---------------- Normalize + build riders ----------------
def normalize_activities(acts, gear_map):
    df = pd.json_normalize(acts, sep=".")
    if df.empty:
        return df

    # datetime
    for col in ("start_date_local", "start_date"):
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=(col=="start_date"))
            # Keep an ISO-like text column for Sheets/joins
            df[col + "_fmt"] = dt.dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")

    # gear
    if "gear_id" in df.columns:
        df["gear_name"] = df["gear_id"].map(lambda g: gear_map.get(g) if gear_map else None)

    # riders
    if "name" in df.columns and "id" in df.columns:
        riders_lists = df["name"].apply(extract_riders_from_name)
        df["other_riders"] = riders_lists.apply(lambda lst: ", ".join(lst) if lst else "")
        df["other_riders_count"] = riders_lists.apply(len)
        df["other_riders_json"] = riders_lists.apply(lambda lst: json.dumps(lst) if lst else "[]")
        print(f"Rider parsing: {(df['other_riders_count']>0).sum()} activities contain riders; "
              f"{int(df['other_riders_count'].sum())} rider names total.")
    else:
        print("normalize_activities: MISSING 'name' or 'id' — rider parsing skipped.")

    # lists/dicts -> JSON strings
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
            df[c] = df[c].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)

    # sort
    sort_col = "start_date" if "start_date" in df.columns else ("start_date_local" if "start_date_local" in df.columns else None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False, kind="mergesort")
    return df

def build_riders_long(df_activities: pd.DataFrame) -> pd.DataFrame:
    cols = ["activity_id", "rider_name"]
    if df_activities.empty or "id" not in df_activities.columns:
        return pd.DataFrame(columns=cols)

    rows = []
    for act_id, riders in zip(df_activities["id"], df_activities.get("other_riders", "").fillna("")):
        if riders:
            for rider in [r.strip() for r in riders.split(",") if r.strip()]:
                rows.append({"activity_id": int(act_id), "rider_name": rider})

    if INCLUDE_SOLO_PLACEHOLDER and "other_riders_count" in df_activities.columns:
        solo_mask = (df_activities["other_riders_count"].fillna(0) == 0) | (df_activities.get("other_riders","").fillna("") == "")
        for act_id in df_activities.loc[solo_mask, "id"]:
            rows.append({"activity_id": int(act_id), "rider_name": " Solo"})

    out = pd.DataFrame(rows, columns=cols)
    print(f"riders_long rows: {len(out)} (includes ' Solo': {INCLUDE_SOLO_PLACEHOLDER})")
    return out

# ---------------- Main ----------------
def main():
    token = get_strava_access_token()
    gear_map = fetch_gear_map(token)

    sh = open_sheet()
    existing = read_existing_activities_df(sh)

    # Determine incremental 'after'
    latest_utc = latest_start_dt_utc(existing)
    if latest_utc is None:
        print("No prior data -> doing a full backfill.")
        acts = fetch_all_activities(token)
    else:
        after_epoch = int((latest_utc - timedelta(seconds=AFTER_SAFETY_BUFFER_S)).timestamp())
        print(f"Incremental mode: after = {after_epoch} ({latest_utc.isoformat()} UTC minus buffer).")
        acts = fetch_activities_after(token, after_epoch)

    # Normalize new batch
    df_new = normalize_activities(acts, gear_map)

    # Merge with existing (by id), keeping newest record if duplicate
    if not existing.empty:
        # ensure consistent id dtype
        if "id" in existing.columns:
            existing["id"] = pd.to_numeric(existing["id"], errors="coerce").dropna().astype("int64")
        if not df_new.empty and "id" in df_new.columns:
            df_new["id"] = pd.to_numeric(df_new["id"], errors="coerce").dropna().astype("int64")

        combined = pd.concat([existing, df_new], ignore_index=True)
        combined = combined.drop_duplicates(subset=["id"], keep="last")
    else:
        combined = df_new.copy()

    combined = combined.sort_values(
        "start_date" if "start_date" in combined.columns else ("start_date_local" if "start_date_local" in combined.columns else "id"),
        ascending=False
    ).reset_index(drop=True)

    # Build riders_long from the combined set so it stays in sync
    df_riders = build_riders_long(combined)

    # Write out
    write_df_to_tab(sh, TAB_RIDERS_LONG, df_riders)
    write_df_to_tab(sh, TAB_ACTIVITIES, combined)

    titles = [ws.title for ws in sh.worksheets()]
    print("Worksheets now present:", titles)
    print("=== PIPELINE DONE ===")

if __name__ == "__main__":
    main()
