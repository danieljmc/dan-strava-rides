# Pull Strava, weather, and geo data to visualize data from activites on Tableau Dashboard.

Link to Tableau Dashboard: https://public.tableau.com/app/profile/daniel.mccomb4807/viz/StravaETL/Dashboard1?publish=yes

Workflow:
1) Extract: Connect to Strava API to read my rides
2) Transform:
  a) Write all fields to sheet
  b) Extract additional riders for anyone after "with" in the name (Due to inconsistent naming, some riders are missed when "with" isn't in the title, some false positives exsit and need to manually be added to stopwords)
  a) Write all fields to sheet "activities_all" tab.
  b) Extract additional riders for anyone after "with" in the name (Due to inconsistent naming, some riders are missed when "with" isn't in the title, some false positives exsit and need to manually be added to stopwords). Writes to "riders_long" tab.
  c) Add line for " Solo" when there are no riders for Tableau filter.  Opted for ETL transformation vs calculated field in Tableau.
4) Load: Connect to Google sheets and write the transformed data
5) Vizulize in Tableau Public: Linked dataset in Tableau togoogle sheets.
5) Connect API to weather and update two tabs
   a) Pulls hourly data for the start/end time of the activity based on latitude and longtitude from Strava. Uses Fall River, MA when no geo data present. Writes to "weather_hourly"
   b) Creates "weather_by_ride" which is an average of weather data by ride.  Joins in tableau by activity ID.
6) Connect API to Geodata and update two tabs.
    a) Pulls geo data by ride (revgeo_cache) and converts to city "geo_by_ride".
7) Vizulize in Tableau Public: Linked dataset in Tableau togoogle sheets.

Files:
pipelyne.py - main python code for the ETL process
pipelyne.py -  python code for the ETL process from Strava
geocode_etl.py -  python code for ETL to get city data
weather_etl.py -  python code for ETL to get weather data
sheets_smoke_test.py - simple test to confirm API access to create the Google Sheets
strava_smoke_test.py - simple test to confirm API access to read from Strava
