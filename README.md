# 🚴‍♂️ Strava → Google Sheets ETL Pipeline  
**Incremental ride ingestion with weather, reverse-geocoding, and Tableau dashboards**

[![Python](https://img.shields.io/badge/python-3.9%2B-blue)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/danieljmc/dan-strava-rides.svg)]()

---

### 🌟 Overview
This project automates extraction of personal **Strava** activity data, enriches it with **weather** and **reverse-geocoded** location details, and writes the results to a connected **Google Sheet** for analysis in **Tableau Public**.

It’s designed to be:
- **Incremental** – only new or missing rides are pulled.
- **Cache-aware** – weather and geo lookups are stored to avoid API overuse.
- **Portfolio-ready** – demonstrates practical ETL, API handling, and BI visualization.

---

## 🏗️ Architecture

```text
          +-------------+        +------------------+
          |  Strava API | -----> |  pipeline.py     |
          +-------------+        |  (activities &   |
                                 |  rider parsing)  |
                                        |
                                        v
                         +--------------------------------+
                         |  Google Sheet (Strava Rides)   |
                         |--------------------------------|
                         | activities_all  | riders_long  |
                         | weather_hourly  | weather_by_ride |
                         | revgeo_cache    | geo_by_ride   |
                         +--------------------------------+
                                        |
                          +------------------------------+
                          | Tableau Public Dashboard     |
                          | (distance, duration, weather) |
                          +------------------------------+
