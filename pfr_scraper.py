import requests
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle

SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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

def clean_schedule(df):
    games = []
    for i in range(0, len(df), 2):
        row1 = df.iloc[i]
        row2 = df.iloc[i+1]
        if pd.isna(row1['Winner/tie']) or pd.isna(row2['Winner/tie']):
            continue
        if row1['Unnamed: 5'] == '@':
            home_team = row2['Winner/tie']
            away_team = row1['Winner/tie']
        else:
            home_team = row1['Winner/tie']
            away_team = row2['Winner/tie']
        games.append({
            'Week': row1['Week'], 'Day': row1['Day'], 'Date': row1['Date'],
            'Time': row1['Time'], 'Away_Team': away_team, 'Home_Team': home_team
        })
    return pd.DataFrame(games)

if __name__ == "__main__":
    print("Authenticating...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    print("\n--- Scraping SCHEDULE ---")
    try:
        schedule_df_raw = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/games.htm")[0]
        schedule_df_clean = clean_schedule(schedule_df_raw)
        write_to_sheet(spreadsheet, "Schedule", schedule_df_clean)
    except Exception as e: print(f"❌ Could not process Schedule: {e}")
    
    # (Add other scraping blocks here for FPI, injuries, etc.)
    
    print("\n✅ Scraper script finished.")