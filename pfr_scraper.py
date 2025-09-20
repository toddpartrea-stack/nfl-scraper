import requests
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
from datetime import datetime

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Google Sheets Authentication ---
def get_gspread_client():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return gspread.authorize(creds)

# --- Helper Function to Write to a Sheet Tab ---
def write_to_sheet(spreadsheet, sheet_name, dataframe):
    print(f"  -> Writing data to '{sheet_name}' tab...")
    if dataframe.empty:
        print(f"  -> Data is empty for {sheet_name}.")
        return
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(dataframe.columns))

    dataframe = dataframe.astype(str).fillna('')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

# --- Main Script ---
if __name__ == "__main__":
    print("Authenticating...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # --- Scraping SCHEDULE (Updated for 1-row format) ---
    print("\n--- Scraping SCHEDULE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/games.htm"
        schedule_df = pd.read_html(url)[0]

        # Create Home and Away team columns from the new format
        schedule_df['Home_Team'] = schedule_df.apply(
            lambda row: row['Loser/tie'] if row['Unnamed: 5'] == '@' else row['Winner/tie'], axis=1
        )
        schedule_df['Away_Team'] = schedule_df.apply(
            lambda row: row['Winner/tie'] if row['Unnamed: 5'] == '@' else row['Loser/tie'], axis=1
        )
        write_to_sheet(spreadsheet, "Schedule", schedule_df)
    except Exception as e:
        print(f"❌ Could not process Schedule: {e}")

    # --- (Add your other working scraper functions back here for FPI, Injuries, etc.) ---

    print("\n✅ Scraper script finished.")
