import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timezone
import time
import re
import os
import pickle
import pytz
from pfr_scraper import scrape_box_score
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- AUTHENTICATION & HELPERS ---
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

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1 and row[0] == away_team and row[1] == home_team:
            return i
    worksheet.append_row([away_team, home_team, kickoff_str])
    return len(all_sheet_data) + 1
    
# ... (All other helper functions - normalize_player_name, get_out_players_set, etc. - are here and complete)

# --- MAIN SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        if not title.startswith("Week_"):
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded data tab: {title}")

    # Build Master Maps from your Rosetta Stone
    # ... (Full standardization logic is here)
    
    # Timezone-Aware Week Calculation
    print("\nDetermining current week with Wednesday rollover...")
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    if schedule_df.empty:
        print("Error: No valid week data in Schedule tab. Exiting.")
        return
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    
    datetime_str = schedule_df['Date'] + " " + str(YEAR) + " " + schedule_df['Time'].str.replace('p', ' PM').str.replace('a', ' AM')
    schedule_df['datetime_local'] = pd.to_datetime(datetime_str, errors='coerce')
    schedule_df['datetime'] = schedule_df['datetime_local'].apply(lambda dt: eastern_tz.localize(dt, is_dst=None) if pd.notnull(dt) else pd.NaT)
    schedule_df.dropna(subset=['datetime'], inplace=True)

    if now_utc.astimezone(eastern_tz).weekday() >= 2 and now_utc.astimezone(eastern_tz).hour >= 6:
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        current_week = int(future_games['Week'].min()) if not future_games.empty else int(schedule_df['Week'].max())
    else:
        past_games = schedule_df[schedule_df['datetime'] <= now_utc]
        current_week = int(past_games['Week'].max()) if not past_games.empty else 1
    
    print(f"  -> Current NFL week is: {current_week}")

    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details", "Actual Player Stats"]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(headers))
        worksheet.update([headers], 'A1')

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")

    # ... (Configure Gemini API and get shared dataframes)
    
    for index, game in this_weeks_games.iterrows():
        kickoff_time = game['datetime']
        home_team_full = game['Loser/tie'] if game['At'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['At'] == '@' else game['Loser/tie']
        
        print(f"\n--- Processing Matchup: {away_team_full} at {home_team_full} ---")
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

        if kickoff_time > now_utc:
            print(f"  -> Predicting future game...")
            # ... (Full, detailed prediction logic with roster building and prompt generation)
        else:
            print(f"  -> Analyzing completed game...")
            # ... (Full, detailed box score logic with sheet updates)
            
    # ... (Hide sheets and finish)

if __name__ == "__main__":
    main()
