import requests
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import io

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
URLS = {
    "defense": f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm",
    "team_offense": f"https://www.pro-football-reference.com/years/{YEAR}/",
    "player_passing": f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm",
    "player_rushing": f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm",
    "player_receiving": f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm",
    "injuries": "https://www.pro-football-reference.com/injuries/injuries.htm",
    "schedule": f"https://www.pro-football-reference.com/years/{YEAR}/games.htm",
    "fpi": "https://www.espn.com/nfl/fpi"
}
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
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
    
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
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        exit()

    # --- Scrape ESPN Football Power Index ---
    print("\n--- Scraping ESPN FPI ---")
    try:
        fpi_df = pd.read_html(URLS["fpi"])[0]
        fpi_df['TEAM'] = fpi_df['TEAM'].str.replace(r'^\d+', '', regex=True).str.strip()
        write_to_sheet(spreadsheet, "FPI", fpi_df)
    except Exception as e: print(f"❌ Could not process FPI Stats: {e}")
    
    # --- Scrape Schedule ---
    print("\n--- Scraping SCHEDULE ---")
    try:
        schedule_df_raw = pd.read_html(URLS["schedule"])[0]
        schedule_df_clean = clean_schedule(schedule_df_raw)
        write_to_sheet(spreadsheet, "Schedule", schedule_df_clean)
    except Exception as e: print(f"❌ Could not process Schedule: {e}")
    
    # ... (Add other scraping sections here if needed, like injuries, player stats, etc.)
    
    print("\n✅ Project script finished.")