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
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
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

def get_top_healthy_player_names(team_df, position, num_players=1):
    """Gets the names of the top N healthy players for a given position from a team's depth chart."""
    position_df = team_df[team_df['Position'] == position]
    healthy_df = position_df[position_df['Status'] == 'Healthy']
    players = healthy_df.head(num_players)['Player'].tolist()
    while len(players) < num_players:
        players.append("[Not Available]")
    return players

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
    
    headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Prediction Analysis", "Actual Winner", "Actual Score"]
    worksheet.update('A1', [headers])
    worksheet.freeze(rows=1)
    fmt = CellFormat(wrapStrategy='WRAP')
    format_cell_range(worksheet, 'F:F', fmt)

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print("--- Initializing Vertex AI ---")
    vertexai.init()
    model = GenerativeModel("gemini-2.5-pro")
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    }
    
    depth_chart_df = dataframes.get('Depth_Charts', pd.DataFrame())
    depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
    depth_chart_df.sort_values(['Team', 'Position', 'Depth'], inplace=True)
    
    player_stats_current = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving'] if name in dataframes], ignore_index=True)
    team_offense_df = dataframes.get('O_Team_Overall', pd.DataFrame())
    
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if pd.notna(row[col]) and row[col]}
    depth_chart_df['Team_Full'] = depth_chart_df['Team'].map(master_team_map)

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        home_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == home_team_full]
        away_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == away_team_full]

        player_map = {
            "home_qb": get_top_healthy_player_names(home_depth_chart, 'QB', 1)[0],
            "away_qb": get_top_healthy_player_names(away_depth_chart, 'QB', 1)[0],
            "home_rb1": get_top_healthy_player_names(home_depth_chart, 'RB', 2)[0],
            "home_rb2": get_top_healthy_player_names(home_depth_chart, 'RB', 2)[1],
            "away_rb1": get_top_healthy_player_names(away_depth_chart, 'RB', 2)[0],
            "away_rb2": get_top_healthy_player_names(away_depth_chart, 'RB', 2)[1],
            "home_wr1": get_top_healthy_player_names(home_depth_chart, 'WR', 3)[0],
            "home_wr2": get_top_healthy_player_names(home_depth_chart, 'WR', 3)[1],
            "home_wr3": get_top_healthy_player_names(home_depth_chart, 'WR', 3)[2],
            "away_wr1": get_top_healthy_player_names(away_depth_chart, 'WR', 3)[0],
            "away_wr2": get_top_healthy_player_names(away_depth_chart, 'WR', 3)[1],
            "away_wr3": get_top_healthy_player_names(away_depth_chart, 'WR', 3)[2],
            "home_te": get_top_healthy_player_names(home_depth_chart, 'TE', 1)[0],
            "away_te": get_top_healthy_player_names(away_depth_chart, 'TE', 1)[0],
        }
        
        player_names_for_stats = [name for name in player_map.values() if name != "[Not Available]"]
        roster_stats = player_stats_current[player_stats_current['Player'].isin(player_names_for_stats)]

        matchup_prompt = f"""
        You are an expert sports analyst and data scientist. Your task is to provide a detailed prediction analysis for an upcoming NFL game.
        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home) using the provided data and the list of key players.
        
        ## Key Players to Predict For:
        - Home QB: {player_map['home_qb']}
        - Away QB: {player_map['away_qb']}
        - Home RB1: {player_map['home_rb1']}, Home RB2: {player_map['home_rb2']}
        - Away RB1: {player_map['away_rb1']}, Away RB2: {player_map['away_rb2']}
        - Home WR1: {player_map['home_wr1']}, Home WR2: {player_map['home_wr2']}, Home WR3: {player_map['home_wr3']}
        - Away WR1: {player_map['away_wr1']}, Away WR2: {player_map['away_wr2']}, Away WR3: {player_map['away_wr3']}
        - Home TE: {player_map['home_te']}
        - Away TE: {player_map['away_te']}

        ## Data for Analysis:
        ### Team Standings ({YEAR}):
        {team_offense_df[team_offense_df['Tm'].isin([home_team_full, away_team_full])].to_string()}
        
        ### Top Healthy Player Stats ({YEAR}):
        {roster_stats.to_string()}
        ---
        Based on your analysis, provide your complete response ONLY as a single, valid JSON object with no markdown.
        Your response **MUST** follow this exact schema. You **MUST** provide a prediction for every key player listed above, unless their name is '[Not Available]'.
        For every 'confidence' field, you **MUST** provide an integer between 1 and 100.

        {{
          "game_prediction": {{ "winner": "string", "winner_confidence": 85, "score": "string", "score_confidence": 70 }},
          "justification": "string",
          "touchdown_scorers": [ {{ "player_name": "string", "confidence": 75 }} ],
          "player_stats": {{
            "{player_map['home_qb']}": {{ "Passing Yards": 0, "Passing Yards_confidence": 0, "Rushing Yards": 0, "Rushing Yards_confidence": 0, "Passing TDs": 0, "Passing TDs_confidence": 0, "Interceptions": 0, "Interceptions_confidence": 0 }},
            "{player_map['away_qb']}": {{ "Passing Yards": 0, "Passing Yards_confidence": 0, "Rushing Yards": 0, "Rushing Yards_confidence": 0, "Passing TDs": 0, "Passing TDs_confidence": 0, "Interceptions": 0, "Interceptions_confidence": 0 }},
            "{player_map['home_rb1']}": {{ "Rushing Yards": 0, "Rushing Yards_confidence": 0, "Rushing TDs": 0, "Rushing TDs_confidence": 0 }},
            "{player_map['away_rb1']}": {{ "Rushing Yards": 0, "Rushing Yards_confidence": 0, "Rushing TDs": 0, "Rushing TDs_confidence": 0 }},
            "{player_map['home_wr1']}": {{ "Receiving Yards": 0, "Receiving Yards_confidence": 0, "Receiving TDs": 0, "Receiving TDs_confidence": 0 }}
            "{player_map['away_wr1']}": {{ "Receiving Yards": 0, "Receiving Yards_confidence": 0, "Receiving TDs": 0, "Receiving TDs_confidence": 0 }}
          }}
        }}
        """
        try:
            response = model.generate_content(matchup_prompt, safety_settings=safety_settings)
            pred_json = json.loads(clean_json_response(response.text))

            game_pred = pred_json.get("game_prediction", {})
            winner = game_pred.get("winner", "N/A")
            score = game_pred.get("score", "N/A")
            
            justification = pred_json.get("justification", "No justification provided.")
            td_scorers = pred_json.get("touchdown_scorers", [])
            player_stats = pred_json.get("player_stats", {})

            analysis_text = f"**1. Game Prediction:**\n"
            analysis_text += f"***Predicted Winner:** {winner} (Confidence: {game_pred.get('winner_confidence', 0)}%)\n"
            analysis_text += f"***Predicted Final Score:** {score} (Confidence: {game_pred.get('score_confidence', 0)}%)\n\n"
            
            analysis_text += f"**2. Key Player Stat Predictions:**\n"
            
            def format_player(name, stats):
                if not stats or name == "[Not Available]": return ""
                p_text = f"***{name}:**\n"
                if 'Passing Yards' in stats: p_text += f"** Passing Yards:** {stats.get('Passing Yards', 'N/A')} (Confidence: {stats.get('Passing Yards_confidence', 0)}%)\n"
                if 'Rushing Yards' in stats: p_text += f"** Rushing Yards:** {stats.get('Rushing Yards', 'N/A')} (Confidence: {stats.get('Rushing Yards_confidence', 0)}%)\n"
                if 'Receiving Yards' in stats: p_text += f"** Receiving Yards:** {stats.get('Receiving Yards', 'N/A')} (Confidence: {stats.get('Receiving Yards_confidence', 0)}%)\n"
                if 'Passing TDs' in stats: p_text += f"** Passing TDs:** {stats.get('Passing TDs', 'N/A')} (Confidence: {stats.get('Passing TDs_confidence', 0)}%)\n"
                if 'Rushing TDs' in stats: p_text += f"** Rushing TDs:** {stats.get('Rushing TDs', 'N/A')} (Confidence: {stats.get('Rushing TDs_confidence', 0)}%)\n"
                if 'Receiving TDs' in stats: p_text += f"** Receiving TDs:** {stats.get('Receiving TDs', 'N/A')} (Confidence: {stats.get('Receiving TDs_confidence', 0)}%)\n"
                if 'Interceptions' in stats: p_text += f"** Interceptions:** {stats.get('Interceptions', 'N/A')} (Confidence: {stats.get('Interceptions_confidence', 0)}%)\n"
                return p_text + "\n"

            for player_key, player_name in player_map.items():
                analysis_text += format_player(player_name, player_stats.get(player_name, {}))

            analysis_text += f"**3. Touchdown Scorers:**\n"
            for scorer in td_scorers:
                player_name = scorer.get("player_name", "N/A")
                confidence = scorer.get("confidence", 0)
                analysis_text += f"** {player_name} (Confidence: {confidence}%)\n"
            analysis_text += "\n"

            analysis_text += f"**4. Justification:**\n{justification}"

            worksheet.update(f'D{row_num}:F{row_num}', [[winner, score, analysis_text.strip()]])
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                print(f"    -> AI Response Finish Reason: {response.candidates[0].finish_reason}")
                print(f"    -> AI Response Safety Ratings: {response.candidates[0].safety_ratings}")
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
