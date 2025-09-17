import requests
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import io
from datetime import datetime

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Google Sheets Authentication (Your working version) ---
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

# --- Helper Function to Write to a Sheet Tab (Your working version) ---
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
    
# --- Advanced Data Cleaning Helper (Your working version) ---
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
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        exit()

    # --- NEW: Scrape TeamRankings.com Power Rankings ---
    print("\n--- Scraping TeamRankings.com Power Rankings ---")
    try:
        url = "https://www.teamrankings.com/nfl/rankings/teams/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        }
        all_tables = pd.read_html(url, header=0)
        rankings_df = all_tables[0]
        write_to_sheet(spreadsheet, "Power_Rankings", rankings_df)
    except Exception as e:
        print(f"❌ Could not process TeamRankings Stats: {e}")

    # --- DYNAMIC INJURY WEEK CALCULATION (Your working version) ---
    season_start_date = datetime(YEAR, 9, 4)
    today = datetime.now()
    days_since_start = (today - season_start_date).days
    current_week = (days_since_start // 7) + 1
    injury_table_title = f"Week {current_week} Injuries"
    print(f"\nCalculated current NFL week: {current_week}. Looking for table: '{injury_table_title}'")
    
    # --- DEFENSE (Your working version) ---
    print("\n--- Scraping DEFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
        if len(all_tables) > 2: write_to_sheet(spreadsheet, "D_Passing", clean_pfr_table(all_tables[2]))
        if len(all_tables) > 3: write_to_sheet(spreadsheet, "D_Rushing", clean_pfr_table(all_tables[3]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    # --- OFFENSE (TEAM) (Your working version) ---
    print("\n--- Scraping TEAM OFFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        try:
            team_offense_df = pd.read_html(url, match="Team Offense")[0]
            write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
        except ValueError:
            print("  -> Team Offense table not found (likely not posted for the new season yet).")
    except Exception as e: print(f"❌ Could not process Team Offensive Stats: {e}")
    
    # --- OFFENSE (PLAYER) (Your working version) ---
    print("\n--- Scraping PLAYER OFFENSE ---")
    try:
        passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm")[0]
        rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm")[0]
        receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm")[0]
        write_to_sheet(spreadsheet, "O_Player_Passing", clean_pfr_table(passing_df))
        write_to_sheet(spreadsheet, "O_Player_Rushing", clean_pfr_table(rushing_df))
        write_to_sheet(spreadsheet, "O_Player_Receiving", clean_pfr_table(receiving_df))
    except Exception as e: print(f"❌ Could not process Player Offensive Stats: {e}")

    # --- INJURIES (Your working version) ---
    print("\n--- Scraping INJURIES ---")
    try:
        url = "https://www.pro-football-reference.com/players/injuries.htm"
        injury_df = pd.read_html(url, match=injury_table_title)[0]
        write_to_sheet(spreadsheet, "Injuries", clean_pfr_table(injury_df))
    except Exception as e: print(f"❌ Could not process Injury Reports: {e}")

    # --- SCHEDULE (Your working version) ---
    print("\n--- Scraping SCHEDULE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/games.htm"
        schedule_df = pd.read_html(url)[0]
        write_to_sheet(spreadsheet, "Schedule", clean_pfr_table(schedule_df))
    except Exception as e: print(f"❌ Could not process Schedule: {e}")

    print("\n✅ Project script finished.")
