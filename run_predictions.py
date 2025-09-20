import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime
import time
import re
import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
YEAR = 2025

# --- Google Sheets Authentication ---
def get_gspread_client():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("ERROR: token.pickle is missing or invalid.")
            return None
    return gspread.authorize(creds)

# --- Function to write predictions to the sheet ---
def write_prediction_to_sheet(spreadsheet, week, away_team, home_team, prediction_text):
    try:
        sheet_name = "Predictions"
        worksheet = spreadsheet.worksheet(sheet_name)
        justification = prediction_text.strip()
        winner_match = re.search(r"Predicted Winner:\s*(.*)", prediction_text)
        score_match = re.search(r"Predicted Final Score:\s*(.*)", prediction_text)
        winner = winner_match.group(1).strip() if winner_match else "See Justification"
        score = score_match.group(1).strip() if score_match else "See Justification"
        worksheet.append_row([week, away_team, home_team, winner, score, justification])
        print(f"  -> ✅ Prediction for {away_team} @ {home_team} written to sheet.")
    except Exception as e:
        print(f"  -> ❌ Error writing prediction to sheet: {e}")

# --- Normalization and Status Functions ---
def normalize_player_name(name):
    if not isinstance(name, str): return ""
    name = name.lower()
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)[\.]*$', '', name) 
    name = re.sub(r'[.\'"+*]', '', name)
    return name.strip()

def get_player_statuses_from_depth_chart(depth_chart_df):
    out_statuses = ['O', 'IR', 'PUP', 'NFI', 'IR-R']
    out_players_df = depth_chart_df[depth_chart_df['Status'].isin(out_statuses)]
    out_players_set = {normalize_player_name(name) for name in out_players_df['Player']}
    print(f"Found {len(out_players_set)} players who are OUT from the depth chart.")
    return out_players_set

# --- Main execution block ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    if not gc: return

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        return

    # Load All Data
    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    try:
        for worksheet in spreadsheet.worksheets():
            title = worksheet.title
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded tab: {title}")
    except Exception as e:
        print(f"❌ Error loading sheet: {e}")
        return
        
    # Check for required tabs
    required_tabs = ['Schedule', 'D_Overall', 'O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving', 'Depth_Charts', 'team_match']
    if not all(tab in dataframes for tab in required_tabs):
        print(f"\n❌ Could not find all necessary data tabs. Found: {list(dataframes.keys())}")
        return
        
    # Build Master Maps from your Rosetta Stone
    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map = {}
    full_name_to_abbr = {}
    for _, row in team_map_df.iterrows():
        full_name = row['Full Name']
        abbr = row['Abbreviation']
        injury_team_name = row['Injury team']
        if full_name: master_team_map[full_name] = full_name
        if abbr: master_team_map[abbr] = full_name
        if injury_team_name: master_team_map[injury_team_name] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    # Standardize All DataFrames using the Master Map
    print("\nStandardizing team names across all data sheets...")
    # FRANCO: Added the new column name to this list
    possible_team_cols = ['Tm', 'Team', 'Winner/tie', 'Loser/tie', 'Unnamed: 1_level_0_Tm', 'Unnamed: 3_level_0_Team']
    for name, df in dataframes.items():
        team_col_found = next((col for col in possible_team_cols if col in df.columns), None)
        if team_col_found:
            df['Team_Full'] = df[team_col_found].map(master_team_map)
            df.dropna(subset=['Team_Full'], inplace=True)
            df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
            print(f"  -> Standardized team names for '{name}' sheet.")

    # Get player statuses and filter "out" players
    print("\nFiltering players based on Depth Chart status...")
    out_players_normalized = get_player_statuses_from_depth_chart(dataframes['Depth_Charts'])
    for stat_sheet_name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']:
        df = dataframes[stat_sheet_name]
        if 'Player' in df.columns:
            normalized_stats_names = df['Player'].apply(normalize_player_name)
            is_out_mask = normalized_stats_names.isin(out_players_normalized)
            removed_players = df[is_out_mask]['Player'].tolist()
            dataframes[stat_sheet_name] = df[~is_out_mask]
            if removed_players:
                print(f"  -> From {stat_sheet_name}, removed {len(removed_players)} players: {removed_players}")

    # Clear old predictions
    try:
        predictions_sheet = spreadsheet.worksheet("Predictions")
        predictions_sheet.clear()
        predictions_sheet.update(range_name='A1:F1', values=[['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])
        print("\nCleared old data from 'Predictions' tab.")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="Predictions", rows=1, cols=6)
        worksheet.update(range_name='A1:F1', values=[['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])

    # Configure Gemini API
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")

    # Find the current week's games
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    # For testing, we are hardcoding the week. To make it dynamic, use the code below.
    current_week = 3
    # DYNAMIC WEEK CALCULATION (use this when you're ready)
    # today = datetime.now()
    # future_games = schedule_df[pd.to_datetime(schedule_df['Date'] + f", {YEAR}", errors='coerce') >= today]
    # current_week = future_games['Week'].min() if not future_games.empty else schedule_df['Week'].max()

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week].dropna(subset=['Winner/tie', 'Loser/tie'])
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
    
    depth_chart_df = dataframes['Depth_Charts']
    depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')

    for index, game in this_weeks_games.iterrows():
        # FRANCO: Fixed the logic to correctly identify home and away teams
        home_team_full = game['Loser/tie'] if game['Unnamed: 5'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['Unnamed: 5'] == '@' else game['Loser/tie']

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

        # Identify key questionable players for the prompt
        key_positions = ['QB', 'RB', 'WR', 'TE']
        questionable_players_df = depth_chart_df[
            (depth_chart_df['Status'] == 'Q') &
            (depth_chart_df['Depth'] == 1) &
            (depth_chart_df['Position'].isin(key_positions)) &
            (depth_chart_df['Team_Full'].isin([home_team_full, away_team_full]))
        ]
        home_q_list = [f"{row['Player']} ({row['Position']})" for i, row in questionable_players_df[questionable_players_df['Team_Full'] == home_team_full].iterrows()]
        away_q_list = [f"{row['Player']} ({row['Position']})" for i, row in questionable_players_df[questionable_players_df['Team_Full'] == away_team_full].iterrows()]
        
        key_questionable_text = f"""
        ## Key Players with Questionable Status
        - {home_team_full}: {', '.join(home_q_list) if home_q_list else 'None'}
        - {away_team_full}: {', '.join(away_q_list) if away_q_list else 'None'}
        """

        # Prepare all other data for the prompt
        home_team_abbr = full_name_to_abbr.get(home_team_full)
        away_team_abbr = full_name_to_abbr.get(away_team_full)
        
        team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
        player_passing_data = dataframes['O_Player_Passing'][dataframes['O_Player_Passing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        player_rushing_data = dataframes['O_Player_Rushing'][dataframes['O_Player_Rushing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        player_receiving_data = dataframes['O_Player_Receiving'][dataframes['O_Player_Receiving']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        Use all of the data provided to make the most informed decision.

        {key_questionable_text}
        ---
        ## {home_team_full} (Home) Data
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == home_team_full].to_string()}
        - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == home_team_abbr].to_string()}
        - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == home_team_abbr].to_string()}
        - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == home_team_abbr].to_string()}

        ## {away_team_full} (Away) Data
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == away_team_full].to_string()}
        - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == away_team_abbr].to_string()}
        - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == away_team_abbr].to_string()}
        - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == away_team_abbr].to_string()}
        ---

        Based on the structured data above, provide the following in a clear format:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Justification:** A brief justification. You MUST specifically mention any "Key Players with Questionable Status" and describe how their potential absence could impact the outcome.
        4. **Key Player Stat Predictions:** Predict stats for key players expected to play provide a confidence percentage from 1% to 100%.
        5. **Touchdown Scorers:** List 2-3 players most likely to score a touchdown provide a confidence percentage from 1% to 100%.
        """
        
        try:
            response = model.generate_content(matchup_prompt)
            print("--- PREDICTION (Raw Text) ---")
            print(response.text)
            write_prediction_to_sheet(spreadsheet, current_week, away_team_full, home_team_full, response.text)
        except Exception as e:
            print(f"Could not generate prediction for this matchup: {e}")
        
        time.sleep(20)

if __name__ == "__main__":
    main()
