import requests
import gspread
import pandas as pd
from datetime import datetime, timezone
import time
import re
import os
import pickle
import pytz
import google.generativeai as genai
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
FOOTBALL_API_KEY = os.getenv('FOOTBALL_API_KEY') # Get the API Football key
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- MANUAL OVERRIDE ---
MANUAL_WEEK_OVERRIDE = None

# --- AUTHENTICATION ---
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

# --- NEW API HELPER ---
def get_game_stats_from_api(game_id):
    print(f"    -> Fetching box score from API for game ID: {game_id}")
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    querystring = {"fixture": str(game_id)}
    headers = {
        "X-RapidAPI-Key": FOOTBALL_API_KEY,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()['response']
        if not data:
            print("    -> FAILED: API returned no stats for this game.")
            return None
        
        # This is a simplified placeholder for parsing the complex JSON
        # For now, we'll just confirm we can get the data
        # A full implementation would parse out player stats here
        
        print("    -> SUCCESS: Fetched data from API.")
        return {"status": "success", "data": data} # Return a success marker
    except Exception as e:
        print(f"    -> An error occurred fetching API box score: {e}")
        return None

# --- HELPER FUNCTIONS ---
def normalize_player_name(name):
    if not isinstance(name, str): return ""
    name = name.lower().replace('.', '').replace("'", "")
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', name).strip()
    return name

def get_out_players_set(depth_chart_df):
    if depth_chart_df.empty: return set()
    out_statuses = ['O', 'IR', 'PUP', 'NFI', 'IR-R']
    out_players_df = depth_chart_df[depth_chart_df['Status'].isin(out_statuses)]
    return {normalize_player_name(name) for name in out_players_df['Player']}

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1:
            if row[0].strip() == away_team and row[1].strip() == home_team:
                return i
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', '', '', ''])
    return len(all_sheet_data) + 1

def run_prediction_mode(spreadsheet, dataframes, full_name_to_abbr, now_utc, week_override=None):
    # This mode is now simplified as it doesn't use historical player data
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']

    if week_override:
        current_week = week_override
    else:
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        if future_games.empty: return
        current_week = int(future_games['Week'].min())

    print(f"  -> Generating predictions for Week {current_week}")
    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details"]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=len(headers))
        worksheet.update('A1', [headers])
        worksheet.freeze(rows=1)

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    team_stats_df = dataframes.get('O_Team_Overall', pd.DataFrame())

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        
        home_stats = team_stats_df[team_stats_df['Tm'] == home_team_full]
        away_stats = team_stats_df[team_stats_df['Tm'] == away_team_full]

        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Predict the outcome of the {away_team_full} at {home_team_full} game.
        Analyze the provided team standings data.
        ---
        ## {home_team_full} (Home) Data
        {home_stats.to_string()}
        ---
        ## {away_team_full} (Away) Data
        {away_stats.to_string()}
        ---
        Based on your analysis, provide the following:
        1. **Predicted Winner:**
        2. **Predicted Final Score:**
        3. **Justification:** A brief justification for your prediction.
        """
        try:
            response = model.generate_content(matchup_prompt)
            # Simplified parsing for the new prompt
            details = response.text
            winner = "See Details"
            score = "See Details"
            # ... a more robust parsing would be needed here
            worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
            worksheet.update(f'H{row_num}', [[details]])
            print(f"    -> SUCCESS: Wrote prediction to sheet.")
        except Exception as e:
            print(f"    -> ERROR: Could not generate prediction: {e}")
        time.sleep(15)

def run_results_mode(spreadsheet, dataframes, full_name_to_abbr, now_utc):
    # This function is now just a placeholder, as the new scraper doesn't get game IDs
    # A full implementation would first fetch the schedule from the API to get game IDs
    print("\n--- TUESDAY: Running in Results Mode (Placeholder) ---")
    print("  -> NOTE: Results mode needs to be fully implemented with API game IDs.")
    pass

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

    required_sheets = ['Schedule', 'team_match']
    if not all(sheet in dataframes for sheet in required_sheets):
        print("❌ CRITICAL ERROR: Could not load required tabs.")
        return

    print("\nStandardizing team names...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    for name, df in dataframes.items():
        team_cols_to_process = [col for col in ['Team', 'Tm'] if col in df.columns]
        if team_cols_to_process:
            for col in team_cols_to_process:
                df[col] = df[col].map(master_team_map).fillna(df[col])

    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_eastern = now_utc.astimezone(eastern_tz)
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str).dt.tz_localize(eastern_tz).dt.tz_convert('UTC')
    dataframes['Schedule'] = schedule_df

    if MANUAL_WEEK_OVERRIDE is not None:
        run_prediction_mode(spreadsheet, dataframes, full_name_to_abbr, now_utc, week_override=MANUAL_WEEK_OVERRIDE)
    elif now_eastern.weekday() == 1:
        run_results_mode(spreadsheet, dataframes, full_name_to_abbr, now_utc)
    else:
        run_prediction_mode(spreadsheet, dataframes, full_name_to_abbr, now_utc)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
