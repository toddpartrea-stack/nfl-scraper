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
    
    # Correctly prepared dataframes from the main() function
    depth_chart_df = dataframes.get('Depth_Charts')
    player_stats_current = dataframes.get('player_stats_current')
    team_offense_df = dataframes.get('O_Team_Overall')

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        
        kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        home_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == home_team_full]
        away_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == away_team_full]

        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_player_names = {p for pos, num in pos_config.items() for p in get_top_healthy_player_names(home_depth_chart, pos, num) if p != "[Not Available]"}
        away_player_names = {p for pos, num in pos_config.items() for p in get_top_healthy_player_names(away_depth_chart, pos, num) if p != "[Not Available]"}
        
        home_roster_stats = player_stats_current[player_stats_current['Player'].isin(home_player_names)]
        away_roster_stats = player_stats_current[player_stats_current['Player'].isin(away_player_names)]

        # ### --- UPGRADE: Stricter prompt to prevent hallucinations --- ###
        matchup_prompt = f"""
        You are an expert sports analyst and data scientist. Your task is to provide a detailed prediction analysis for an upcoming NFL game.
        **Your primary directive is to base your analysis exclusively on the data provided below. Do not use any prior knowledge.**

        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home).

        ## Data for Analysis:
        ### Team Standings ({YEAR}):
        {team_offense_df[team_offense_df['Team_Full'].isin([home_team_full, away_team_full])].to_string()}
        
        ### Home Team - Healthy Player Stats ({YEAR}):
        {home_roster_stats.to_string()}

        ### Away Team - Healthy Player Stats ({YEAR}):
        {away_roster_stats.to_string()}
        ---
        Based on your analysis of ONLY the data provided, provide your complete response as a single, valid JSON object with no markdown.

        Your response must contain keys for "game_prediction", "justification", "top_performers", and "touchdown_scorers".
        
        - In "top_performers", you MUST include the starting QB and top RB for each team. You can then add 1-2 other impactful players (WRs/TEs) from each team.
        - The "defensive_td_prediction" should be an object with a boolean 'will_occur' and a 'confidence' integer.
        - For every 'confidence' field, you MUST provide an integer between 1 and 100.
        """
        try:
            response = model.generate_content(matchup_prompt, safety_settings=safety_settings)
            pred_json = json.loads(clean_json_response(response.text))

            # ... (Formatting logic remains the same)
            
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                print(f"    -> AI Response Finish Reason: {response.candidates[0].finish_reason}")
                print(f"    -> AI Response Safety Ratings: {response.candidates[0].safety_ratings}")
        time.sleep(5)

# The run_results_mode is removed as per your request

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
                df = pd.DataFrame(data[1:], columns=data[0])
                if 'Player' in df.columns:
                    df['Player'] = df['Player'].str.strip()
                dataframes[title] = df

    required_data_sheets = ['Schedule', 'team_match', 'O_Player_Passing']
    if not all(sheet in dataframes for sheet in required_data_sheets):
        print("❌ CRITICAL ERROR: Could not load required player data tabs.")
        return

    # ### --- UPGRADE: Data Unification Step --- ###
    print("\n--- Unifying Team Names Across All Data Sources ---")
    team_map_df = dataframes['team_match']
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if pd.notna(row[col]) and row[col]}
    
    # Apply the master map to all relevant dataframes
    for name, df in dataframes.items():
        team_col = None
        if 'Tm' in df.columns:
            team_col = 'Tm'
        elif 'Team' in df.columns:
            team_col = 'Team'
        
        if team_col:
            # Create the unified 'Team_Full' column
            df['Team_Full'] = df[team_col].map(master_team_map)
            # Fill any non-matches with the original name to be safe
            df['Team_Full'].fillna(df[team_col], inplace=True)
            dataframes[name] = df

    # Re-build player_stats_current with unified names, although this is less critical
    dataframes['player_stats_current'] = pd.concat([
        dataframes.get('O_Player_Passing', pd.DataFrame()),
        dataframes.get('O_Player_Rushing', pd.DataFrame()),
        dataframes.get('O_Player_Receiving', pd.DataFrame())
    ], ignore_index=True)


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

    print("\n--- Running PREDICTION mode for upcoming week. ---")
    run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=MANUAL_WEEK_OVERRIDE)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction/Results script finished.")

if __name__ == "__main__":
    main()
