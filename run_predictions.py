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

# ... (Helper functions are unchanged) ...
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
    if stats_df.empty or depth_chart_df.empty: return pd.DataFrame(), pd.DataFrame()
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    team_depth_chart['Depth'] = pd.to_numeric(team_depth_chart['Depth'], errors='coerce')
    team_depth_chart.sort_values('Depth', inplace=True)
    active_roster_players = []
    top_healthy_players = pd.DataFrame()
    for pos, num_players in pos_config.items():
        pos_depth_all = team_depth_chart[team_depth_chart['Position'] == pos]
        healthy_players = pos_depth_all[pos_depth_all['Status'] == 'Healthy']
        active_players_for_pos = healthy_players.head(num_players)
        active_roster_players.extend(active_players_for_pos['Player'].tolist())
        top_healthy_players = pd.concat([top_healthy_players, healthy_players.head(1)])
    roster_stats_df = stats_df[stats_df['Player'].isin(active_roster_players)]
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
        # ### --- NEW DIAGNOSTIC CODE --- ###
        print("\n--- DEBUG INFO ---")
        print(f"Current UTC time the script is using: {now_utc}")
        print("Latest 5 game times from schedule (in UTC):")
        # Display the 5 latest games from the schedule to check dates
        print(schedule_df.nlargest(5, 'datetime')[['datetime', 'Week', 'Away Team', 'Home Team']])
        print("--------------------\n")
        
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        if not future_games.empty:
            current_week = int(future_games['Week'].min())

    if not current_week:
        print("  -> No future games found to predict.")
        return

    print(f"  -> Generating predictions for Week {current_week}")
    # ... (Rest of the function is unchanged)
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
        home_qb_name = home_top_players[home_top_players['Position'] == 'QB']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'QB'].empty else "[Not Available]"
        away_qb_name = away_top_players[away_top_players['Position'] == 'QB']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'QB'].empty else "[Not Available]"
        home_rb_name = home_top_players[home_top_players['Position'] == 'RB']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'RB'].empty else "[Not Available]"
        away_rb_name = away_top_players[away_top_players['Position'] == 'RB']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'RB'].empty else "[Not Available]"
        home_wr_name = home_top_players[home_top_players['Position'] == 'WR']['Player'].iloc[0] if not home_top_players[home_top_players['Position'] == 'WR'].empty else "[Not Available]"
        away_wr_name = away_top_players[away_top_players['Position'] == 'WR']['Player'].iloc[0] if not away_top_players[away_top_players['Position'] == 'WR'].empty else "[Not Available]"
        matchup_prompt = f"""
        You are an expert sports analyst and data scientist...
        """
        try:
            response = model.generate_content(matchup_prompt)
            pred_json = json.loads(clean_json_response(response.text))
            winner = pred_json.get("winner", "N/A")
            score = pred_json.get("score", "N/A")
            justification = pred_json.get("justification", "No justification provided.")
            td_scorers = pred_json.get("touchdown_scorers", [])
            player_stats = pred_json.get("player_stats", {})
            analysis_text = f"**1. Game Prediction:**\n"
            analysis_text += f"***Predicted Winner:** {winner}\n"
            analysis_text += f"***Predicted Final Score:** {score}\n\n"
            analysis_text += f"**2. Key Player Stat Predictions:**\n"
            def format_player(name, stats):
                if not stats: return ""
                p_text = f"***{name}:**\n"
                if 'Passing Yards' in stats: p_text += f"** Passing Yards:** {stats.get('Passing Yards')} (Likelihood: {stats.get('Passing Yards Likelihood')})\n"
                if 'Rushing Yards' in stats: p_text += f"** Rushing Yards:** {stats.get('Rushing Yards')} (Likelihood: {stats.get('Rushing Yards Likelihood')})\n"
                if 'Receiving Yards' in stats: p_text += f"** Receiving Yards:** {stats.get('Receiving Yards')} (Likelihood: {stats.get('Receiving Yards Likelihood')})\n"
                if 'Passing TDs' in stats: p_text += f"** Passing TDs:** {stats.get('Passing TDs')} (Likelihood: {stats.get('Passing TDs Likelihood')})\n"
                if 'Rushing TDs' in stats: p_text += f"** Rushing TDs:** {stats.get('Rushing TDs')} (Likelihood: {stats.get('Rushing TDs Likelihood')})\n"
                if 'Receiving TDs' in stats: p_text += f"** Receiving TDs:** {stats.get('Receiving TDs')} (Likelihood: {stats.get('Receiving TDs Likelihood')})\n"
                if 'Interceptions' in stats: p_text += f"** Interceptions:** {stats.get('Interceptions')} (Likelihood: {stats.get('Interceptions Likelihood')})\n"
                return p_text + "\n"
            analysis_text += format_player(home_qb_name, player_stats.get(home_qb_name, {})); analysis_text += format_player(away_qb_name, player_stats.get(away_qb_name, {})); analysis_text += format_player(home_rb_name, player_stats.get(home_rb_name, {})); analysis_text += format_player(away_rb_name, player_stats.get(away_rb_name, {})); analysis_text += format_player(home_wr_name, player_stats.get(home_wr_name, {})); analysis_text += format_player(away_wr_name, player_stats.get(away_wr_name, {}))
            analysis_text += f"**3. Touchdown Scorers:**\n"
            for scorer in td_scorers:
                analysis_text += f"** {scorer}\n"
            analysis_text += "\n"
            analysis_text += f"**4. Justification:**\n{justification}"
            worksheet.update(f'D{row_num}:F{row_num}', [[winner, score, analysis_text.strip()]])
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
        time.sleep(5)

def run_results_mode(spreadsheet, dataframes, now_utc):
    # This function is unchanged
    pass

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
    
    schedule_df = dataframes['Schedule']
    schedule_df = schedule_df[schedule_df['Date'] != 'Date'].copy()
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M', errors='coerce').dt.tz_localize('UTC')
    schedule_df.dropna(subset=['datetime'], inplace=True)
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    dataframes['Schedule'] = schedule_df
    
    now_eastern = now_utc.astimezone(eastern_tz)
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
