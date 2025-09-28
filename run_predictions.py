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
    if stats_df.empty or depth_chart_df.empty: return pd.DataFrame(), pd.DataFrame() # Return two empty dataframes
    
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    team_depth_chart['Depth'] = pd.to_numeric(team_depth_chart['Depth'], errors='coerce')
    team_depth_chart.sort_values('Depth', inplace=True)

    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth_all = team_depth_chart[team_depth_chart['Position'] == pos]
        healthy_players = pos_depth_all[pos_depth_all['Status'] == 'Healthy']
        active_players_for_pos = healthy_players.head(num_players)
        active_roster_players.extend(active_players_for_pos['Player'].tolist())
            
    roster_stats_df = stats_df[stats_df['Player'].isin(active_roster_players)]
    
    # Also return a dataframe of the top healthy players themselves for names
    top_healthy_players = pd.DataFrame()
    for pos in pos_config.keys():
        pos_depth_all = team_depth_chart[team_depth_chart['Position'] == pos]
        healthy_players = pos_depth_all[pos_depth_all['Status'] == 'Healthy']
        top_healthy_players = pd.concat([top_healthy_players, healthy_players.head(1)])

    return roster_stats_df, top_healthy_players

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
        
        pos_config = {'QB': 1, 'RB': 1, 'WR': 1}
        home_roster_stats, home_top_players = get_game_day_roster(home_team_full, player_stats_current, depth_chart_df, pos_config)
        away_roster_stats, away_top_players = get_game_day_roster(away_team_full, player_stats_current, depth_chart_df, pos_config)
        
        # Get top player names, providing a default if a position is missing
        home_qb_name = home_top_players[home_top_players['Position'] == 'QB']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'QB'].empty else "[Not Available]"
        away_qb_name = away_top_players[away_top_players['Position'] == 'QB']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'QB'].empty else "[Not Available]"
        home_rb_name = home_top_players[home_top_players['Position'] == 'RB']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'RB'].empty else "[Not Available]"
        away_rb_name = away_top_players[away_top_players['Position'] == 'RB']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'RB'].empty else "[Not Available]"
        home_wr_name = home_top_players[home_top_players['Position'] == 'WR']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'WR'].empty else "[Not Available]"
        away_wr_name = away_top_players[away_top_players['Position'] == 'WR']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'WR'].empty else "[Not Available]"

        matchup_prompt = f"""
        You are an expert sports analyst and data scientist. Your task is to provide a detailed prediction analysis for an upcoming NFL game.
        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home) using the provided data and player names.
        
        The top healthy players at key positions are:
        - Home QB: {home_qb_name}
        - Away QB: {away_qb_name}
        - Home RB: {home_rb_name}
        - Away RB: {away_rb_name}
        - Home WR: {home_wr_name}
        - Away WR: {away_wr_name}

        ## Data for Analysis:
        ### {home_team_full} (Home)
        - **Team Standings ({YEAR}):** {home_team_stats.to_string()}
        - **Top Healthy Player Stats ({YEAR}):** {home_roster_stats.to_string()}
        ---
        ### {away_team_full} (Away)
        - **Team Standings ({YEAR}):** {away_team_stats.to_string()}
        - **Top Healthy Player Stats ({YEAR}):** {away_roster_stats.to_string()}
        ---
        Based on your analysis, provide your complete response ONLY as a single, valid JSON object with no markdown.
        The JSON must contain keys for "winner", "score", "justification", "touchdown_scorers", and "player_stats".
        The "player_stats" value must be an object containing keys for each player you were given, with predicted stats for each.
        """
        try:
            response = model.generate_content(matchup_prompt)
            pred_json = json.loads(clean_json_response(response.text))

            # ### --- UPGRADE: PYTHON-POWERED FORMATTING --- ###
            # Read the reliable data from the JSON
            winner = pred_json.get("winner", "N/A")
            score = pred_json.get("score", "N/A")
            justification = pred_json.get("justification", "No justification provided.")
            td_scorers = pred_json.get("touchdown_scorers", [])
            player_stats = pred_json.get("player_stats", {})

            # Build the beautiful markdown text block using the clean data
            analysis_text = f"**1. Game Prediction:**\n"
            analysis_text += f"***Predicted Winner:** {winner}\n"
            analysis_text += f"***Predicted Final Score:** {score}\n\n"
            
            analysis_text += f"**2. Key Player Stat Predictions:**\n"
            # Helper to format player stats
            def format_player(name, stats):
                p_text = f"***{name}:**\n"
                p_text += f"** Passing Yards:** {stats.get('Passing Yards', 'N/A')} (Likelihood: {stats.get('Passing Yards Likelihood', 'N/A')})\n" if 'Passing Yards' in stats else ""
                p_text += f"** Rushing Yards:** {stats.get('Rushing Yards', 'N/A')} (Likelihood: {stats.get('Rushing Yards Likelihood', 'N/A')})\n" if 'Rushing Yards' in stats else ""
                p_text += f"** Receiving Yards:** {stats.get('Receiving Yards', 'N/A')} (Likelihood: {stats.get('Receiving Yards Likelihood', 'N/A')})\n" if 'Receiving Yards' in stats else ""
                p_text += f"** Passing TDs:** {stats.get('Passing TDs', 'N/A')} (Likelihood: {stats.get('Passing TDs Likelihood', 'N/A')})\n" if 'Passing TDs' in stats else ""
                p_text += f"** Rushing TDs:** {stats.get('Rushing TDs', 'N/A')} (Likelihood: {stats.get('Rushing TDs Likelihood', 'N/A')})\n" if 'Rushing TDs' in stats else ""
                p_text += f"** Receiving TDs:** {stats.get('Receiving TDs', 'N/A')} (Likelihood: {stats.get('Receiving TDs Likelihood', 'N/A')})\n" if 'Receiving TDs' in stats else ""
                p_text += f"** Interceptions:** {stats.get('Interceptions', 'N/A')} (Likelihood: {stats.get('Interceptions Likelihood', 'N/A')})\n" if 'Interceptions' in stats else ""
                return p_text + "\n"

            analysis_text += format_player(home_qb_name, player_stats.get(home_qb_name, {}))
            analysis_text += format_player(away_qb_name, player_stats.get(away_qb_name, {}))
            analysis_text += format_player(home_rb_name, player_stats.get(home_rb_name, {}))
            analysis_text += format_player(away_rb_name, player_stats.get(away_rb_name, {}))
            analysis_text += format_player(home_wr_name, player_stats.get(home_wr_name, {}))
            analysis_text += format_player(away_wr_name, player_stats.get(away_wr_name, {}))

            analysis_text += f"**3. Touchdown Scorers:**\n"
            for scorer in td_scorers:
                analysis_text += f"** {scorer}\n"
            analysis_text += "\n"

            analysis_text += f"**4. Justification:**\n{justification}"

            # Update the sheet with the separated winner/score and the perfectly formatted text block
            worksheet.update(f'D{row_num}:F{row_num}', [[winner, score, analysis_text.strip()]])
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
        time.sleep(5)

def run_results_mode(spreadsheet, dataframes, now_utc):
    # This function is unchanged
    pass

def main():
    # This function is unchanged
    pass

if __name__ == "__main__":
    main()
