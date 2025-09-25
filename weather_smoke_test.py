# weather_smoke_test.py
import os, requests, datetime as dt

def om_hourly(lat, lon, date_iso):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,cloudcover,windspeed_10m,winddirection_10m,weathercode",
        "start_date": date_iso,
        "end_date": date_iso,
        "timezone": "auto",
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    lat = float(os.getenv("HOME_LAT", "41.636"))  # fallback if you like
    lon = float(os.getenv("HOME_LON", "-71.176"))
    date_iso = (dt.date.today() - dt.timedelta(days=7)).isoformat()  # last week as a sample
    data = om_hourly(lat, lon, date_iso)
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])[:5]
    temps = hourly.get("temperature_2m", [])[:5]
    clouds = hourly.get("cloudcover", [])[:5]
    wind = hourly.get("windspeed_10m", [])[:5]
    dirs  = hourly.get("winddirection_10m", [])[:5]
    print("Open-Meteo sample for", date_iso)
for t, temp, cc, ws, wd in zip(times, temps, clouds, winds, dirs):
    print(f"{t} | temp {temp}°C | cloud {cc}% | wind {ws} m/s @ {wd}°")
    print("OK: fetched", len(hourly.get("time", [])), "hourly rows.")

if __name__ == "__main__":
    main()
