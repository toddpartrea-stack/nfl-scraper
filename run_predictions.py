import os
import json
import re
import time
import pandas as pd
import pytz
import gspread
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

# NEW: Import the Vertex AI library
import vertexai
from vertexai.generative_models import GenerativeModel

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_HOST = "v1.american-football.api-sports.io"
YEAR = 2025
MANUAL_WEEK_OVERRIDE = None

# NEW: Get Google Cloud Project ID from secrets
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# --- AUTHENTICATION & HELPERS ---
def get_gspread_client():
    creds_json_str = os.getenv('GSPREAD_CREDENTIALS')
    if not creds_json_str:
        raise ValueError("GSPREAD_CREDENTIALS secret not found.")
    creds_dict = json.loads(creds_json_str)
    client = gspread.service_account_from_dict(creds_dict)
    return client

def get_api_data(endpoint, params):
    url = f"https://{API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get('response', [])
    except requests.exceptions.RequestException as e:
        print(f"  -> API request failed for endpoint '{endpoint}': {e}")
        return []

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
        if sheet.title.startswith("Week_"):
            try: sheet.show()
            except Exception: pass
        else:
            try: sheet.hide()
            except Exception: pass

def clean_json_response(text):
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        return match.group(1)
    return text.strip()

def run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=None):
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']
    team_map_df = dataframes['team_match']
    
    current_week = 0
    if week_override:
        current_week = week_override
    else:
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        if not future_games.empty:
            current_week = int(future_games['Week'].min())

    if not current_week:
        print("  -> No future games found to predict.")
        return

    print(f"  -> Generating predictions for Week {current_week}")
    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        time.sleep(1)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=8)
    
    headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details (JSON)"]
    worksheet.update('A1', [headers])
    worksheet.freeze(rows=1)

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    # NEW: Initialize Vertex AI
    print("--- Initializing Vertex AI ---")
    vertexai.init(project=GCP_PROJECT_ID, location="us-central1")
    model = GenerativeModel("gemini-1.0-pro")
    
    depth_chart_df = dataframes.get('Depth_Charts', pd.DataFrame())
    player_stats_current = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving'] if name in dataframes], ignore_index=True)
    player_stats_previous = pd.concat([dataframes.get(name, pd.DataFrame()) for name in [f'{YEAR-1}_O_Player_Passing', f'{YEAR-1}_O_Player_Rushing', f'{YEAR-1}_O_Player_Receiving'] if name in dataframes], ignore_index=True)
    team_offense_df = dataframes.get('O_Team_Overall', pd.DataFrame())
    
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if pd.notna(row[col]) and row[col]}

    for df, team_col in [(depth_chart_df, 'Team'), (player_stats_current, 'Tm'), (player_stats_previous, 'Tm'), (team_offense_df, 'Tm')]:
        if not df.empty and team_col in df.columns:
            df['Team_Full'] = df[team_col].map(master_team_map)

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_roster_stats = get_game_day_roster(home_team_full, player_stats_current, depth_chart_df, pos_config)
        away_roster_stats = get_game_day_roster(away_team_full, player_stats_current, depth_chart_df, pos_config)
        home_hist_stats = player_stats_previous[player_stats_previous['Player'].isin(home_roster_stats['Player'])]
        away_hist_stats = player_stats_previous[player_stats_previous['Player'].isin(away_roster_stats['Player'])]
        home_team_stats = team_offense_df[team_offense_df['Team_Full'] == home_team_full]
        away_team_stats = team_offense_df[team_offense_df['Team_Full'] == away_team_full]
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Predict the outcome of the {away_team_full} at {home_team_full} game.
        Analyze the provided data tables below, prioritizing current season data.
        ## {home_team_full} (Home) Data
        - Team Standings ({YEAR}): {home_team_stats.to_string()}
        - Active Roster Stats ({YEAR}): {home_roster_stats.to_string()}
        - Roster Historical Stats ({YEAR-1}): {home_hist_stats.to_string()}
        ---
        ## {away_team_full} (Away) Data
        - Team Standings ({YEAR}): {away_team_stats.to_string()}
        - Active Roster Stats ({YEAR}): {away_roster_stats.to_string()}
        - Roster Historical Stats ({YEAR-1}): {away_hist_stats.to_string()}
        ---
        Based on your analysis, provide your complete response ONLY as a single, valid JSON object. Do not include any text before or after the JSON.
        The JSON object must have the following keys: "predicted_winner", "predicted_score", "confidence_percent", "justification", "touchdown_scorers".
        - "predicted_winner": string (The full team name of the winner)
        - "predicted_score": string (The final score, e.g., "27-24")
        - "confidence_percent": integer (A number from 1 to 100)
        - "justification": string (A brief 2-3 sentence analysis)
        - "touchdown_scorers": array of strings (A list of 2-3 players most likely to score a TD)
        """
        try:
            response = model.generate_content(matchup_prompt)
            cleaned_response = clean_json_response(response.text)
            pred_json = json.loads(cleaned_response)
            winner = pred_json.get("predicted_winner", "N/A")
            score = pred_json.get("predicted_score", "N/A")
            worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
            worksheet.update(f'H{row_num}', [[json.dumps(pred_json, indent=2)]])
            print(f"    -> SUCCESS: Wrote prediction to sheet. Winner: {winner}, Score: {score}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
        time.sleep(5)

def run_results_mode(spreadsheet, dataframes, now_utc):
    schedule_df = dataframes['Schedule']
    eastern_tz = pytz.timezone('US/Eastern')
    past_games = schedule_df[schedule_df['datetime'] <= now_utc]
    if past_games.empty:
        print("  -> No past games found to update results.")
        return
    last_week_number = int(past_games['Week'].max())
    print(f"  -> Updating results for Week {last_week_number}")
    games_to_update = schedule_df[schedule_df['Week'] == last_week_number]
    pred_sheet_name = f"Week_{last_week_number}_Predictions"
    try:
        worksheet_pred = spreadsheet.worksheet(pred_sheet_name)
    except gspread.WorksheetNotFound:
        print(f"  -> Prediction sheet for Week {last_week_number} not found. No results to update.")
        return
    stats_sheet_name = f"Week_{last_week_number}_Actual_Stats"
    try:
        worksheet_stats = spreadsheet.worksheet(stats_sheet_name)
        worksheet_stats.clear()
    except gspread.WorksheetNotFound:
        worksheet_stats = spreadsheet.add_worksheet(title=stats_sheet_name, rows=500, cols=20)
    stats_headers = ["Matchup", "Player", "Team", "PassYds", "PassTD", "RushYds", "RushTD", "RecYds", "RecTD"]
    worksheet_stats.update('A1', [stats_headers])
    worksheet_stats.freeze(rows=1)
    all_player_stats_for_week = []
    for index, game in games_to_update.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        game_id = game['GameID']
        print(f"\n--- Updating: {away_team_full} at {home_team_full} (GameID: {game_id}) ---")
        game_details = get_api_data("games", {"id": game_id})
        if not game_details: continue
        game_data = game_details[0]
        final_score = f"{game_data['scores']['away']['total']}-{game_data['scores']['home']['total']}"
        actual_winner = "Tie"
        if game_data['scores']['home']['total'] > game_data['scores']['away']['total']:
            actual_winner = home_team_full
        elif game_data['scores']['away']['total'] > game_data['scores']['home']['total']:
            actual_winner = away_team_full
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet_pred, away_team_full, home_team_full, kickoff_display_str)
        worksheet_pred.update(f'F{row_num}:G{row_num}', [[actual_winner, final_score]])
        game_player_stats = get_api_data("games/statistics/players", {"id": game_id})
        if game_player_stats:
            for team_data in game_player_stats:
                team_name = team_data.get('team', {}).get('name')
                for player_data in team_data.get('players', []):
                    p_info = {'Matchup': f"{away_team_full} @ {home_team_full}", 'Player': player_data.get('player', {}).get('name'), 'Team': team_name, 'PassYds': 0, 'PassTD': 0, 'RushYds': 0, 'RushTD': 0, 'RecYds': 0, 'RecTD': 0}
                    for group in player_data.get('statistics', []):
                        group_name = group.get('group')
                        stats = {s['name']: s['value'] for s in group.get('statistics', [])}
                        if group_name == 'passing':
                            p_info['PassYds'] = stats.get('passing yards', 0); p_info['PassTD'] = stats.get('passing touchdowns', 0)
                        elif group_name == 'rushing':
                            p_info['RushYds'] = stats.get('rushing yards', 0); p_info['RushTD'] = stats.get('rushing touchdowns', 0)
                        elif group_name == 'receiving':
                            p_info['RecYds'] = stats.get('receiving yards', 0); p_info['RecTD'] = stats.get('receiving touchdowns', 0)
                    all_player_stats_for_week.append(p_info)
        time.sleep(2)
    if all_player_stats_for_week:
        stats_df = pd.DataFrame(all_player_stats_for_week)[stats_headers]
        worksheet_stats.append_rows(stats_df.values.tolist(), value_input_option='USER_ENTERED')
        print(f"\n  -> Wrote {len(stats_df)} player stat lines to '{stats_sheet_name}'.")

def main():
    if not all([API_KEY, GCP_PROJECT_ID]):
        print("❌ CRITICAL ERROR: Required secrets not found.")
        return

    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    
    dataframes = {}
    print("\nLoading all data from Google Sheet tabs...")
    sheet_titles = [s.title for s in spreadsheet.worksheets()]
    for title in sheet_titles:
        if not title.startswith("Week_"):
            print(f"  -> Loading '{title}'...")
            worksheet = spreadsheet.worksheet(title)
            data = worksheet.get_all_values()
            if data and len(data) > 1:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])

    required_data_sheets = ['Schedule', 'team_match', 'O_Player_Passing']
    if not all(sheet in dataframes for sheet in required_data_sheets):
        print("❌ CRITICAL ERROR: Could not load required player data tabs.")
        return
    
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_eastern = now_utc.astimezone(eastern_tz)
    
    schedule_df = dataframes['Schedule']
    schedule_df = schedule_df[schedule_df['Date'] != 'Date'].copy()
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M', errors='coerce').dt.tz_localize('UTC')
    schedule_df.dropna(subset=['datetime'], inplace=True)
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    dataframes['Schedule'] = schedule_df

    if MANUAL_WEEK_OVERRIDE is not None:
        run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=MANUAL_WEEK_OVERRIDE)
    elif now_eastern.weekday() == 1:
        run_results_mode(spreadsheet, dataframes, now_utc)
    else:
        run_prediction_mode(spreadsheet, dataframes, now_utc)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction/Results script finished.")


if __name__ == "__main__":
    main()
