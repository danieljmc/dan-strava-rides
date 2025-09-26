# strava_smoke_test.py
import os, sys, requests, pprint


REQUIRED = ["STRAVA_CLIENT_ID", "STRAVA_CLIENT_SECRET", "STRAVA_REFRESH_TOKEN"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print("ERROR: missing GitHub Secrets / env:", ", ".join(missing))
    sys.exit(2)

CID  = os.environ["STRAVA_CLIENT_ID"]
SEC  = os.environ["STRAVA_CLIENT_SECRET"]
RT_OLD = os.environ["STRAVA_REFRESH_TOKEN"]
GITHUB_STEP_SUMMARY = os.getenv("GITHUB_STEP_SUMMARY")  # special Actions file

def mask_token(tok: str, head=6, tail=4) -> str:
    if not tok or len(tok) <= head + tail:
        return "****"
    return f"{tok[:head]}…{tok[-tail:]}"

def write_summary(msg: str):
    # Writes to the run's "Summary" tab in GitHub Actions (if available)
    if GITHUB_STEP_SUMMARY:
        with open(GITHUB_STEP_SUMMARY, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

def get_access_token():
    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CID,
            "client_secret": SEC,
            "grant_type": "refresh_token",
            "refresh_token": RT_OLD,
        },
        timeout=30,
    )
    print("Token refresh status:", r.status_code)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    if r.status_code != 200:
        print("Refresh error payload:", data)
        sys.exit(3)

    scope = data.get("scope")
    print("Token scope:", scope)

    rt_new = data.get("refresh_token")
    if rt_new and rt_new != RT_OLD:
        # Mask the token in logs; also add a clear message in the run summary.
        masked = mask_token(rt_new)
        print(f"NOTICE: Strava rotated your refresh_token. New (masked): {masked}")
        write_summary(f"### 🔁 Strava refresh_token rotated\nNew token (masked): `{masked}`\n\n"
                      f"👉 Update your GitHub secret **STRAVA_REFRESH_TOKEN** to the new value.\n")
    return data["access_token"]

def list_activities(token, per_page=5):
    r = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {token}"},
        params={"per_page": per_page, "page": 1},
        timeout=30,
    )
    print("Activities status:", r.status_code)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        print("Activities error payload:", data)
        sys.exit(4)
    return data

def main():
    token = get_access_token()
    acts = list_activities(token, 5)
    print(f"Fetched {len(acts)} activities.")
    pp = pprint.PrettyPrinter(width=120, sort_dicts=False)
    for i, a in enumerate(acts, 1):
        row = {
            "Activity ID": a.get("id"),
            "Activity Date": a.get("start_date_local") or a.get("start_date"),
            "Activity Name": a.get("name"),
            "Activity Type": a.get("sport_type") or a.get("type"),
            "Distance (m)": a.get("distance"),
        }
        print(f"\n=== Activity {i} ===")
        pp.pprint(row)

if __name__ == "__main__":
    main()
