import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timezone, date
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
    
# ... (Add any other necessary helper functions like normalize_player_name if needed)

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

    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    print("\nStandardizing team names across all data sheets...")
    possible_team_cols = ['Away Team', 'Home Team']
    for name, df in dataframes.items():
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
        for col in possible_team_cols:
            if col in df.columns:
                df[col] = df[col].map(master_team_map).fillna(df[col])
        if 'Team_Full' not in df.columns and 'Team' in df.columns:
            df['Team_Full'] = df['Team'].map(master_team_map).fillna(df['Team'])
            if 'Team_Full' in df.columns:
                 df.dropna(subset=['Team_Full'], inplace=True)
                 df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)

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
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")
    
    for index, game in this_weeks_games.iterrows():
        away_team_full = game['Away Team']
        home_team_full = game['Home Team']
        kickoff_time = game['datetime']
        boxscore_link = game.get('Boxscore', '')
        
        print(f"\n--- Processing Matchup: {away_team_full} at {home_team_full} ---")
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

        if kickoff_time > now_utc:
            print(f"  -> Predicting future game...")
            # This is where you would build the full prompt with all the data
            matchup_prompt = f"Provide a detailed NFL prediction for {away_team_full} at {home_team_full}."
            
            try:
                response = model.generate_content(matchup_prompt)
                details = response.text
                # This is where you would parse the response
                winner, score = "AI Prediction", "AI Score"
                worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
                worksheet.update(f'H{row_num}', [[details]])
                print(f"    -> Wrote prediction for {away_team_full} at {home_team_full}")
            except Exception as e:
                print(f"    -> Could not generate prediction: {e}")
        else:
            print(f"  -> Analyzing completed game...")
            if boxscore_link and 'boxscores' in boxscore_link:
                full_boxscore_url = "https://www.pro-football-reference.com" + boxscore_link
                box_score_data = scrape_box_score(full_boxscore_url)
                if box_score_data:
                     # This logic needs to be updated based on the new schedule format
                     actual_winner = "TBD" 
                     actual_score = box_score_data['final_score']
                     worksheet.update(f'F{row_num}:G{row_num}', [[actual_winner, actual_score]])
                     # This is where you would write the actual player stats
                     print(f"    -> Updated actuals for {away_team_full} at {home_team_full}")
            else:
                print(f"    -> Box score link not found for this game.")
            
    # hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
