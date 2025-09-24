# strava_smoke_test.py
import os, time, requests, pprint

CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["STRAVA_REFRESH_TOKEN"]

def get_access_token():
    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def list_activities(access_token, per_page=5):
    r = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"per_page": per_page, "page": 1},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def fetch_gear_map(access_token):
    r = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    m = {}
    for bike in (data.get("bikes") or []):
        if bike.get("id"):
            m[bike["id"]] = bike.get("name") or bike.get("brand_name") or "Bike"
    for shoe in (data.get("shoes") or []):
        if shoe.get("id"):
            m[shoe["id"]] = shoe.get("name") or shoe.get("brand_name") or "Shoes"
    return m

def main():
    access = get_access_token()
    acts = list_activities(access, per_page=5)
    gear = fetch_gear_map(access)

    print(f"Fetched {len(acts)} activities.")
    pp = pprint.PrettyPrinter(width=120, sort_dicts=False)
    for i, a in enumerate(acts, 1):
        row = {
            "Activity ID": a.get("id"),
            "Activity Date": a.get("start_date_local") or a.get("start_date"),
            "Activity Name": a.get("name"),
            "Activity Type": a.get("sport_type") or a.get("type"),
            "Distance (m)": a.get("distance"),
            "Activity Gear": gear.get(a.get("gear_id")) if a.get("gear_id") else None,
            "Elapsed Time (s)": a.get("elapsed_time"),
            "Moving Time (s)": a.get("moving_time"),
            "Max Speed (m/s)": a.get("max_speed"),
            "Average Speed (m/s)": a.get("average_speed"),
            "Elevation Gain (m)": a.get("total_elevation_gain"),
            "Elevation Low (m)": a.get("elev_low"),
            "Elevation High (m)": a.get("elev_high"),
            "Calories": a.get("calories"),
        }
        print(f"\n=== Activity {i} ===")
        pp.pprint(row)

if __name__ == "__main__":
    main()
