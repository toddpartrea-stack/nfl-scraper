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
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_HOST = "v1.american-football.api-sports.io"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- MANUAL OVERRIDE ---
MANUAL_WEEK_OVERRIDE = None

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

def get_api_data(endpoint, params):
    url = f"https://{API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()['response']

def get_game_day_roster(team_full_name, stats_df, depth_chart_df, pos_config):
    if stats_df.empty or depth_chart_df.empty: return pd.DataFrame()
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].head(num_players)
        active_roster_players.extend(pos_depth['Player'].tolist())
    roster_df = stats_df[stats_df['Player'].isin(active_roster_players)]
    return roster_df

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1 and row[0].strip() == away_team and row[1].strip() == home_team:
            return i
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', '', '', ''])
    return len(all_sheet_data) + 1

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if not sheet.title.startswith("Week_"):
            try: sheet.hide()
            except Exception: pass
        else:
            try: sheet.show()
            except Exception: pass

def run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=None):
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']
    team_map_df = dataframes['team_match']
    
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
    player_stats_2025 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']], ignore_index=True)
    player_stats_2024 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['2024_O_Player_Passing', '2024_O_Player_Rushing', '2024_O_Player_Receiving']], ignore_index=True)
    team_offense_2025 = dataframes.get('O_Team_Overall', pd.DataFrame())
    
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if row[col]}
    for df in [depth_chart_df, player_stats_2025, player_stats_2024, team_offense_2025]:
        if not df.empty:
            team_col = 'Team' if 'Team' in df.columns else 'Tm'
            if team_col in df.columns:
                df['Team_Full'] = df[team_col].map(master_team_map)

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_roster = get_game_day_roster(home_team_full, player_stats_2025, depth_chart_df, pos_config)
        away_roster = get_game_day_roster(away_team_full, player_stats_2025, depth_chart_df, pos_config)
        
        home_hist_stats = player_stats_2024[player_stats_2024['Player'].isin(home_roster['Player'])]
        away_hist_stats = player_stats_2024[player_stats_2024['Player'].isin(away_roster['Player'])]
        
        home_team_stats = team_offense_2025[team_offense_2025['Team_Full'] == home_team_full]
        away_team_stats = team_offense_2025[team_offense_2025['Team_Full'] == away_team_full]
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Predict the outcome of the {away_team_full} at {home_team_full} game.
        **Analysis Guidelines:**
        - Prioritize current {YEAR} season data as the primary indicator of team form.
        - Use {YEAR-1} data as supplementary context for players.
        Analyze the provided data tables below.
        ---
        ## {home_team_full} (Home) Data
        - Team Standings ({YEAR}): {home_team_stats.to_string()}
        - Active Player Stats ({YEAR}): {home_roster.to_string()}
        - Previous Season Stats ({YEAR-1}): {home_hist_stats.to_string()}
        ---
        ## {away_team_full} (Away) Data
        - Team Standings ({YEAR}): {away_team_stats.to_string()}
        - Active Player Stats ({YEAR}): {away_roster.to_string()}
        - Previous Season Stats ({YEAR-1}): {away_hist_stats.to_string()}
        ---
        Based on your analysis, provide the following in a clear format:
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
            worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
            worksheet.update(f'H{row_num}', [[details]])
            print(f"    -> SUCCESS: Wrote prediction to sheet.")
        except Exception as e:
            print(f"    -> ERROR: Could not generate prediction: {e}")
        time.sleep(15)

def run_results_mode(spreadsheet, dataframes, now_utc):
    schedule_df = dataframes['Schedule']
    eastern_tz = pytz.timezone('US/Eastern')
    past_games = schedule_df[schedule_df['datetime'] <= now_utc]
    if past_games.empty: return
    last_week_number = int(past_games['Week'].max())
    print(f"  -> Updating results for Week {last_week_number}")
    games_to_update = schedule_df[schedule_df['Week'] == last_week_number]
    
    pred_sheet_name = f"Week_{last_week_number}_Predictions"
    try:
        worksheet_pred = spreadsheet.worksheet(pred_sheet_name)
    except gspread.WorksheetNotFound:
        print(f"  -> Prediction sheet for Week {last_week_number} not found. Running predictions first.")
        run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=last_week_number)
        worksheet_pred = spreadsheet.worksheet(pred_sheet_name)

    stats_sheet_name = f"Week_{last_week_number}_Actual_Stats"
    try:
        worksheet_stats = spreadsheet.worksheet(stats_sheet_name)
        worksheet_stats.clear()
    except gspread.WorksheetNotFound:
        worksheet_stats = spreadsheet.add_worksheet(title=stats_sheet_name, rows=500, cols=20)
    
    stats_headers = ["Matchup", "Player", "PassYds", "RushYds", "RecYds", "PassTD", "RushTD", "RecTD"]
    worksheet_stats.update('A1', [stats_headers])
    worksheet_stats.freeze(rows=1)

    for index, game in games_to_update.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        game_id = game['GameID']
        print(f"\n--- Updating: {away_team_full} at {home_team_full} ---")
        
        game_details = get_api_data("games", {"id": game_id})
        if not game_details: continue
        game_data = game_details[0]
        final_score = f"{game_data['scores']['away']['total']}-{game_data['scores']['home']['total']}"
        actual_winner = home_team_full if game_data['scores']['home']['total'] > game_data['scores']['away']['total'] else away_team_full
        
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet_pred, away_team_full, home_team_full, kickoff_display_str)
        worksheet_pred.update(f'F{row_num}:G{row_num}', [[actual_winner, final_score]])
        
        player_stats_data = get_api_data("players/statistics", {"game": game_id})
        if player_stats_data:
            stats_list = []
            for team_stats in player_stats_data:
                for player_data in team_stats['players']:
                    p_info = {'Matchup': f"{away_team_full} @ {home_team_full}", 'Player': player_data['player']['name']}
                    for group in player_data['statistics']:
                        for stat in group['statistics']:
                            if stat['name'] == 'passing yards': p_info['PassYds'] = stat['value']
                            # Add more stats here...
                    stats_list.append(p_info)
            
            if stats_list:
                stats_df = pd.DataFrame(stats_list)
                worksheet_stats.append_rows(stats_df.values.tolist())
        time.sleep(2)

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
        
    team_map_df = dataframes['team_match']
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if row[col]}
    
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_eastern = now_utc.astimezone(eastern_tz)
    
    schedule_df = dataframes['Schedule']
    schedule_df = schedule_df[schedule_df['Date'] != 'Date'].copy()
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str, errors='coerce').dt.tz_localize(eastern_tz, ambiguous='infer').dt.tz_convert('UTC')
    schedule_df.dropna(subset=['Week', 'datetime'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    dataframes['Schedule'] = schedule_df

    if MANUAL_WEEK_OVERRIDE is not None:
        run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=MANUAL_WEEK_OVERRIDE)
    elif now_eastern.weekday() == 1:
        run_results_mode(spreadsheet, dataframes, now_utc)
    else:
        run_prediction_mode(spreadsheet, dataframes, now_utc)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
