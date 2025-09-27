import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timezone
import time
import re
import os
import pickle
import pytz
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
FOOTBALL_API_KEY = os.getenv('FOOTBALL_API_KEY')
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

def get_game_day_roster(team_full_name, team_abbr, depth_chart_df, stats_df, out_players_set, pos_config):
    if stats_df.empty or depth_chart_df.empty: return pd.DataFrame()
    player_col = next((c for c in stats_df.columns if 'Player' in c), 'Player')
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players: break
            player_name_normalized = normalize_player_name(player_row['Player'])
            if player_name_normalized not in out_players_set:
                active_roster_players.append({'Player_Normalized': player_name_normalized, 'Player': player_row['Player'], 'Pos': player_row['Position']})
                healthy_players_found += 1
    if not active_roster_players: return pd.DataFrame()
    active_roster_df = pd.DataFrame(active_roster_players)
    stats_df['Player_Normalized'] = stats_df[player_col].apply(normalize_player_name)
    merged_df = pd.merge(active_roster_df, stats_df, on='Player_Normalized', how='left')
    for col in merged_df.columns:
        if pd.api.types.is_numeric_dtype(merged_df[col]):
            merged_df[col] = merged_df[col].fillna(0)
    if 'Player_x' in merged_df.columns:
        merged_df['Player'] = merged_df['Player_x'].fillna(merged_df['Player_y'])
        merged_df['Pos'] = merged_df['Pos_x'].fillna(merged_df['Pos_y'])
    merged_df['Team_Abbr'] = team_abbr
    final_columns = ['Player', 'Team_Abbr', 'Pos']
    stat_cols_to_add = [c for c in stats_df.columns if c not in ['Player', 'Player_Normalized', 'Team_Abbr', 'Pos', 'Tm', 'Player_x', 'Player_y', 'Pos_x', 'Pos_y']]
    final_columns.extend(stat_cols_to_add)
    final_columns_exist = [c for c in final_columns if c in merged_df.columns]
    return merged_df[final_columns_exist]

def get_historical_stats(current_roster_df, team_abbr, historical_df):
    if historical_df.empty or current_roster_df.empty: return pd.DataFrame()
    player_col_hist = next((c for c in historical_df.columns if 'Player' in c), 'Player')
    player_col_curr = next((c for c in current_roster_df.columns if 'Player' in c), 'Player')
    historical_df['Player_Normalized'] = historical_df[player_col_hist].apply(normalize_player_name)
    current_roster_df['Player_Normalized'] = current_roster_df[player_col_curr].apply(normalize_player_name)
    active_players_normalized = list(current_roster_df['Player_Normalized'])
    historical_roster = historical_df[historical_df['Player_Normalized'].isin(active_players_normalized)].copy()
    if 'Team_Abbr' in historical_roster.columns: historical_roster['Team_Abbr'] = team_abbr
    if 'Tm' in historical_roster.columns: historical_roster['Tm'] = team_abbr
    return historical_roster.drop(columns=['Player_Normalized'], errors='ignore')

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if not sheet.title.startswith("Week_"):
            try:
                sheet.hide()
                print(f"  -> Hid '{sheet.title}' sheet.")
            except Exception: pass
        else:
            try:
                sheet.show()
                print(f"  -> Ensured '{sheet.title}' is visible.")
            except Exception: pass

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1:
            if row[0].strip() == away_team and row[1].strip() == home_team:
                return i
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', '', '', ''])
    return len(all_sheet_data) + 1

def run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=None):
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']
    team_map_df = dataframes['team_match']
    full_name_to_abbr = {row['Full Name']: row['Abbreviation'] for _, row in team_map_df.iterrows()}

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
    depth_chart_df = dataframes.get('Depth_Charts', pd.DataFrame())
    if not depth_chart_df.empty: depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
    out_players_set = get_out_players_set(depth_chart_df)
    player_stats_2025 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']], ignore_index=True)
    player_stats_2024 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['2024_O_Player_Passing', '2024_O_Player_Rushing', '2024_O_Player_Receiving']], ignore_index=True)
    team_offense_2025 = dataframes.get('O_Team_Overall', pd.DataFrame())

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        home_team_abbr, away_team_abbr = full_name_to_abbr.get(home_team_full), full_name_to_abbr.get(away_team_full)
        if not home_team_abbr or not away_team_abbr:
            print(f"    -> ERROR: Could not find team abbreviation for '{home_team_full}' or '{away_team_full}'. Skipping game.")
            continue
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, player_stats_2025, out_players_set, pos_config)
        away_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, player_stats_2025, out_players_set, pos_config)
        home_hist = get_historical_stats(home_roster, home_team_abbr, player_stats_2024)
        away_hist = get_historical_stats(away_roster, away_team_abbr, player_stats_2024)
        home_team_off_2025 = team_offense_2025[team_offense_2025['Tm'] == home_team_full]
        away_team_off_2025 = team_offense_2025[team_offense_2025['Tm'] == away_team_full]
        matchup_prompt = f"""
        Act as an expert NFL analyst. Predict the outcome of the {away_team_full} at {home_team_full} game.
        **Analysis Guidelines:**
        - **Prioritize current {YEAR} season data** as the primary indicator of team form.
        - Use {YEAR-1} data as supplementary context for players.
        - Acknowledge that early-season data is limited. Base your analysis on performance within the games played so far.
        Analyze the provided data tables below.
        ---
        ## {home_team_full} (Home) Data
        - Team Standings ({YEAR}): {home_team_off_2025.to_string()}
        - Active Player Stats ({YEAR}): {home_roster.to_string()}
        - Previous Season Stats ({YEAR-1}): {home_hist.to_string()}
        ---
        ## {away_team_full} (Away) Data
        - Team Standings ({YEAR}): {away_team_off_2025.to_string()}
        - Active Player Stats ({YEAR}): {away_roster.to_string()}
        - Previous Season Stats ({YEAR-1}): {away_hist.to_string()}
        ---
        Based on your analysis following the guidelines, provide the following in a clear format:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Justification:** A brief justification for your prediction.
        4. **Key Player Stat Predictions:** For the starting QB, RB, and top WR for each team, provide predictions.
        5. **Touchdown Scorers:** List 2-3 players most likely to score a rushing or receiving touchdown.
        """
        try:
            response = model.generate_content(matchup_prompt)
            details = response.text
            winner_match = re.search(r"Predicted Winner:\s*(.*)", details, re.IGNORECASE)
            score_match = re.search(r"Predicted Final Score:\s*(.*)", details, re.IGNORECASE)
            winner = winner_match.group(1).strip() if winner_match else "See Details"
            score = score_match.group(1).strip() if score_match else "See Details"
            worksheet.update([[winner, score]], f'D{row_num}:E{row_num}')
            worksheet.update([[details]], f'H{row_num}')
            print(f"    -> SUCCESS: Wrote prediction to sheet.")
        except Exception as e:
            print(f"    -> ERROR: Could not generate prediction: {e}")
        time.sleep(15)

def run_results_mode(spreadsheet, dataframes, now_utc):
    # This is a placeholder for the final API-driven results logic
    print("\n--- TUESDAY: Running in Results Mode (Placeholder) ---")
    print("  -> NOTE: Results mode will be implemented next.")
    pass

def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    try:
        for worksheet in spreadsheet.worksheets():
            title = worksheet.title
            if not title.startswith("Week_"):
                data = worksheet.get_all_values()
                if data:
                    dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        print(f"❌ A critical error occurred while reading sheets: {e}")
        return
    required_sheets = ['Schedule', 'team_match']
    if not all(sheet in dataframes for sheet in required_sheets):
        print("❌ CRITICAL ERROR: Could not load required tabs.")
        return
        
    team_map_df = dataframes['team_match']
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if row[col]}
    full_name_to_abbr = {row['Full Name']: row['Abbreviation'] for _, row in team_map_df.iterrows() if row['Full Name'] and row['Abbreviation']}

    print("\nStandardizing team names...")
    for name, df in dataframes.items():
        team_cols = [col for col in ['Team', 'Tm'] if col in df.columns]
        for col in team_cols:
            df['Team_Full'] = df[col].map(master_team_map)
    
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_eastern = now_utc.astimezone(eastern_tz)
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str, errors='coerce').dt.tz_localize(eastern_tz, ambiguous='infer').dt.tz_convert('UTC')
    schedule_df.dropna(subset=['Week', 'datetime'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    dataframes['Schedule'] = schedule_df

    if MANUAL_WEEK_OVERRIDE is not None:
        run_prediction
