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
import vertexai
from vertexai.generative_models import GenerativeModel
# UPDATED: Importing the correct formatting functions
from gspread_formatting import CellFormat, format_cell_range

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_HOST = "v1.american-football.api-sports.io"
YEAR = 2025
MANUAL_WEEK_OVERRIDE = None

# --- AUTHENTICATION & HELPERS ---
def get_gspread_client():
    credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_path:
        raise ValueError("Could not find Google credentials path. The auth step in the workflow may have failed.")
    return gspread.service_account(filename=credential_path)

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
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', ''])
    return len(all_sheet_data) + 1

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if sheet.title.startswith("Week_") or sheet.title == "Todds Tab":
            try:
                sheet.show()
            except Exception:
                pass
        else:
            try:
                sheet.hide()
            except Exception:
                pass

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
    
    headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Prediction Analysis", "Actual Winner", "Actual Score"]
    worksheet.update('A1', [headers])
    worksheet.freeze(rows=1)

    # ### --- FINAL FIX: Using the correct method for text wrapping --- ###
    fmt = CellFormat(wrapStrategy='WRAP')
    format_cell_range(worksheet, 'F:F', fmt)

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print("--- Initializing Vertex AI ---")
    vertexai.init()
    model = GenerativeModel("gemini-1.5-pro")
    
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
        
        pos_config = {'QB': 1, 'RB': 1, 'WR': 1, 'TE': 1}
        home_roster_stats = get_game_day_roster(home_team_full, player_stats_current, depth_chart_df, pos_config)
        away_roster_stats = get_game_day_roster(away_team_full, player_stats_current, depth_chart_df, pos_config)
        home_hist_stats = player_stats_previous[player_stats_previous['Player'].isin(home_roster_stats['Player'])]
        away_hist_stats = player_stats_previous[player_stats_previous['Player'].isin(away_roster_stats['Player'])]
        home_team_stats = team_offense_df[team_offense_df['Team_Full'] == home_team_full]
        away_team_stats = team_offense_df[team_offense_df['Team_Full'] == away_team_full]
        
        matchup_prompt = f"""
        You are an expert sports analyst and gambler with a deep understanding of NFL data. Your task is to provide a detailed prediction analysis for an upcoming game.
        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home) using the provided data.
        ## Data for Analysis:
        ### {home_team_full} (Home)
        - **Team Standings ({YEAR}):** {home_team_stats.to_string()}
        - **Top Player Stats ({YEAR}):** {home_roster_stats.to_string()}
        ---
        ### {away_team_full} (Away)
        - **Team Standings ({YEAR}):** {away_team_stats.to_string()}
        - **Top Player Stats ({YEAR}):** {away_roster_stats.to_string()}
        ---
        Based on your analysis, provide your complete response as a single block of markdown-formatted text. Your response must follow this structure exactly, including all asterisks, numbering, and spacing:

        **1. Game Prediction:**
        ***Predicted Winner:** [Team Name]
        ***Predicted Final Score:** [Team A Score] - [Team B Score]

        **2. Key Player Stat Predictions:**
        ***[Home Team QB Name]:**
        ** Passing Yards:** [Yards] (Likelihood: [Percent])
        ** Rushing Yards:** [Yards] (Likelihood: [Percent])
        ** Passing TDs:** [TDs] (Likelihood: [Percent])
        ** Interceptions:** [INTs] (Likelihood: [Percent])

        ***[Away Team QB Name]:**
        ** Passing Yards:** [Yards] (Likelihood: [Percent])
        ** Rushing Yards:** [Yards] (Likelihood: [Percent])
        ** Passing TDs:** [TDs] (Likelihood: [Percent])
        ** Interceptions:** [INTs] (Likelihood: [Percent])

        ***[Home Team Lead RB Name]:**
        ** Rushing Yards:** [Yards] (Likelihood: [Percent])
        ** Rushing TDs:** [TDs] (Likelihood: [Percent])

        ***[Away Team Lead RB Name]:**
        ** Rushing Yards:** [Yards] (Likelihood: [Percent])
        ** Rushing TDs:** [TDs] (Likelihood: [Percent])

        ***[Home Team Lead WR Name]:**
        ** Receiving Yards:** [Yards] (Likelihood: [Percent])
        ** Receiving TDs:** [TDs] (Likelihood: [Percent])

        ***[Away Team Lead WR Name]:**
        ** Receiving Yards:** [Yards] (Likelihood: [Percent])
        ** Receiving TDs:** [TDs] (Likelihood: [Percent])

        **3. Touchdown Scorers:**
        ** [Player Name 1] ([Position])
        ** [Player Name 2] ([Position])

        **4. Justification:**
        [Your concise, expert-level summary explaining the key reasons for your decision, referencing specific data points or player matchups.]
        """
        try:
            response = model.generate_content(matchup_prompt)
            
            prediction_text = response.text
            
            winner_match = re.search(r"Predicted Winner:\s*\*\*(.*?)\*\*", prediction_text)
            score_match = re.search(r"Predicted Final Score:\s*\*\*(.*?)\*\*", prediction_text)
            
            winner = winner_match.group(1).strip() if winner_match else "N/A"
            score = score_match.group(1).strip() if score_match else "N/A"

            worksheet.update(f'D{row_num}:F{row_num}', [[winner, score, prediction_text]])
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
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

    try:
        pred_headers = worksheet_pred.row_values(1)
        actual_winner_col_index = pred_headers.index("Actual Winner") + 1
        actual_score_col_index = pred_headers.index("Actual Score") + 1
    except (ValueError, IndexError):
        worksheet_pred.update('G1:H1', [['Actual Winner', 'Actual Score']])
        actual_winner_col_index = 7
        actual_score_col_index = 8
        
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
        worksheet_pred.update_cell(row_num, actual_winner_col_index, actual_winner)
        worksheet_pred.update_cell(row_num, actual_score_col_index, final_score)
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
    if not API_KEY:
        print("❌ CRITICAL ERROR: API_KEY secret not found.")
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
