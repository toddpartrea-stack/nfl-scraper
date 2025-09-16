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
    
# --- Advanced Data Cleaning Helper ---
def clean_pfr_table(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk'].copy()
    df = df.dropna(how='all').reset_index(drop=True)
    return df

# --- Main Script ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # --- DYNAMIC INJURY WEEK CALCULATION ---
    season_start_date = datetime(YEAR, 9, 4)
    today = datetime.now()
    days_since_start = (today - season_start_date).days
    current_week = (days_since_start // 7) + 1
    injury_table_title = f"Week {current_week} Injuries"
    print(f"Calculated current NFL week: {current_week}. Looking for table: '{injury_table_title}'")
    
    # --- NEW: Get ESPN FPI data from the reliable API ---
    print("\n--- Getting ESPN FPI from API ---")
    try:
        fpi_url = "http://site.api.espn.com/apis/v2/sports/football/nfl/fpi"
        response = requests.get(fpi_url)
        response.raise_for_status()
        fpi_data = response.json()['teams']
        
        # Process the clean JSON data
        teams_data = [item['team'] for item in fpi_data]
        stats_data = [item['stats'] for item in fpi_data]
        
        fpi_df = pd.json_normalize(teams_data)
        stats_df = pd.json_normalize(stats_data)
        
        final_fpi_df = pd.concat([fpi_df[['displayName', 'abbreviation']], stats_df], axis=1)
        final_fpi_df.rename(columns={'displayName': 'Team'}, inplace=True)

        write_to_sheet(spreadsheet, "FPI", final_fpi_df)
    except Exception as e: 
        print(f"❌ Could not process FPI API data: {e}")

    # --- Your working PFR scraping logic ---
    print("\n--- Scraping Pro Football Reference Data ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        team_offense_df = pd.read_html(url, match="Team Offense")[0]
        write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
    except Exception as e: print(f"❌ Could not process Team Offensive Stats: {e}")
    
    try:
        passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm")[0]
        rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm")[0]
        receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm")[0]
        write_to_sheet(spreadsheet, "O_Player_Passing", clean_pfr_table(passing_df))
        write_to_sheet(spreadsheet, "O_Player_Rushing", clean_pfr_table(rushing_df))
        write_to_sheet(spreadsheet, "O_Player_Receiving", clean_pfr_table(receiving_df))
    except Exception as e: print(f"❌ Could not process Player Offensive Stats: {e}")

    try:
        url = "https://www.pro-football-reference.com/players/injuries.htm"
        injury_df = pd.read_html(url, match=injury_table_title)[0]
        write_to_sheet(spreadsheet, "Injuries", clean_pfr_table(injury_df))
    except Exception as e: print(f"❌ Could not process Injury Reports: {e}")

    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/games.htm"
        schedule_df = pd.read_html(url)[0]
        write_to_sheet(spreadsheet, "Schedule", clean_pfr_table(schedule_df))
    except Exception as e: print(f"❌ Could not process Schedule: {e}")

    print("\n✅ Scraper script finished.")