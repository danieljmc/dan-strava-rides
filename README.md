# dan-strava-rides
ETL for pulling my Strava Data and combing Weather Data.  Post to Google Sheets that will link to public Tableau viz.

Workflow:
1) Extract: Connect to Strava API to read my rides
2) Transform:
  a) Write all fields to sheet
  b) Extract additional riders for anyone after "with" in the name (Due to inconsistent naming, some riders are missed when "with" isn't in the title, some false positives exsit and need to manually be added to stopwords)
  c) Add line for " Solo" when there are no riders for Tableau filter.  Opted for ETL transformation vs calculated field in Tableau.
4) Load: Connect to Google sheets and write the transformed data
5) Vizulize in Tableau Public: Linked dataset in Tableau togoogle sheets.

Files:
pipelyne.py - main python code for the ETL process
sheets_smoke_test.py - simple test to confirm API access to create the Google Sheets
strava_smoke_test.py - simple test to confirm API access to read from Strava
