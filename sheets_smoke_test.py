# sheets_smoke_test.py  
import os, sys, json, datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd

REQUIRED = ["GCP_SVC_JSON", "SHEET_ID"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print("ERROR: missing GitHub Secrets / env:", ", ".join(missing))
    sys.exit(2)

# Build creds from the JSON secret
svc_info = json.loads(os.environ["GCP_SVC_JSON"])
svc_email = svc_info.get("client_email")
print(f"Service account email: {svc_email or '(unknown)'}")  # Helpful for sharing

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(svc_info, scopes=scopes)

# Connect
gc = gspread.authorize(creds)

# Open target sheet by key
sheet_id = os.environ["SHEET_ID"]
sh = gc.open_by_key(sheet_id)

# Create or get a worksheet named "smoke"
TAB = "smoke"
try:
    ws = sh.worksheet(TAB)
    print(f"Found worksheet '{TAB}'.")
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TAB, rows="100", cols="20")
    print(f"Created worksheet '{TAB}'.")

# Append a test row with timestamp
now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
df = pd.DataFrame([{
    "when": now,
    "message": "hello from GitHub Actions 🚀",
    "note": "You can delete or keep this tab."
}])

# If sheet is empty, write headers; otherwise append below existing rows
existing = ws.get_all_values()
if not existing:
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
else:
    # Compute next row index (1-based with header assumed on row 1)
    next_row = len(existing) + 1
    ws.update(f"A{next_row}", [df.iloc[0].tolist()])

print("Smoke test wrote 1 row to the 'smoke' worksheet.")
