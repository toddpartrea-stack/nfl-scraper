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
        winner_match = re.search(r"\*?\*?Predicted Winner:\*?\*?\s*(.*)", prediction_text)
        score_match = re.search(r"\*?\*?Predicted Final Score:\*?\*?\s*(.*)", prediction_text)
        winner = winner_match.group(1).strip() if winner_match else "See Justification"
        score = score_match.group(1).strip() if score_match else "See Justification"
        worksheet.append_row([week, away_team, home_team, winner, score, justification])
        print(f"  -> ✅ Prediction for {away_team} @ {home_team} written to sheet.")
    except Exception as e:
        print(f"  -> ❌ Error writing prediction to sheet: {e}")

# --- Helper Functions ---
def normalize_player_name(name):
    if not isinstance(name, str): return ""
    name = name.lower()
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)[\.]*$', '', name) 
    name = re.sub(r'[.\'"+*]', '', name)
    return name.strip()

def get_out_players_set(depth_chart_df):
    out_statuses = ['O', 'IR', 'PUP', 'NFI', 'IR-R']
    out_players_df = depth_chart_df[depth_chart_df['Status'].isin(out_statuses)]
    out_players_set = {normalize_player_name(name) for name in out_players_df['Player']}
    print(f"Found {len(out_players_set)} players who are OUT from the depth chart.")
    return out_players_set

def get_game_day_roster(team_full_name, team_abbr, depth_chart_df, stats_df, out_players_set, pos_config):
    player_col = 'Player' if 'Player' in stats_df.columns else 'Unnamed: 1_level_0_Player'
    if player_col not in stats_df.columns: return pd.DataFrame()
    
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    team_depth_chart['Player_Normalized'] = team_depth_chart['Player'].apply(normalize_player_name)
    stats_df['Player_Normalized'] = stats_df[player_col].apply(normalize_player_name)
    
    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players: break
            if player_row['Player_Normalized'] not in out_players_set:
                active_roster_players.append(player_row['Player_Normalized'])
                healthy_players_found += 1
    
    final_roster_df = stats_df[stats_df['Player_Normalized'].isin(active_roster_players)].copy()
    
    if 'Team_Abbr' in final_roster_df.columns:
        final_roster_df['Team_Abbr'] = team_abbr
    if 'Tm' in final_roster_df.columns:
        final_roster_df['Tm'] = team_abbr

    return final_roster_df.drop(columns=['Player_Normalized'], errors='ignore')

def get_historical_stats(current_roster_df, team_abbr, historical_df):
    if historical_df.empty or current_roster_df.empty: return pd.DataFrame()
    
    player_col_hist = 'Player' if 'Player' in historical_df.columns else 'Unnamed: 1_level_0_Player'
    player_col_curr = 'Player' if 'Player' in current_roster_df.columns else 'Unnamed: 1_level_0_Player'

    if player_col_hist not in historical_df.columns or player_col_curr not in current_roster_df.columns:
        return pd.DataFrame()

    historical_df['Player_Normalized'] = historical_df[player_col_hist].apply(normalize_player_name)
    current_roster_df['Player_Normalized'] = current_roster_df[player_col_curr].apply(normalize_player_name)
    
    active_players_normalized = list(current_roster_df['Player_Normalized'])
    historical_roster = historical_df[historical_df['Player_Normalized'].isin(active_players_normalized)].copy()

    if 'Team_Abbr' in historical_roster.columns:
        historical_roster['Team_Abbr'] = team_abbr
    if 'Tm' in historical_roster.columns:
        historical_roster['Tm'] = team_abbr
        
    return historical_roster.drop(columns=['Player_Normalized'], errors='ignore')

# --- Main execution block ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # Load All Data
    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        data = worksheet.get_all_values()
        if data:
            dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
            print(f"  -> Loaded tab: {title}")
        
    # Build Master Maps from your Rosetta Stone
    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    # Standardize All DataFrames
    print("\nStandardizing team names across all data sheets...")
    possible_team_cols = ['Tm', 'Team', 'Winner/tie', 'Loser/tie', 'Unnamed: 1_level_0_Tm', 'Unnamed: 3_level_0_Team']
    for name, df in dataframes.items():
        team_col_found = next((col for col in possible_team_cols if col in df.columns), None)
        if team_col_found:
            df['Team_Full'] = df[team_col_found].map(master_team_map)
            df.dropna(subset=['Team_Full'], inplace=True)
            df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
            print(f"  -> Standardized team names for '{name}' sheet.")

    # Get the master list of all players who are out
    depth_chart_df = dataframes['Depth_Charts']
    depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
    out_players_set = get_out_players_set(depth_chart_df)
    
    # Clear old predictions and Configure Gemini API
    try:
        predictions_sheet = spreadsheet.worksheet("Predictions")
        predictions_sheet.clear()
        predictions_sheet.update(range_name='A1:F1', values=[['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])
        print("\nCleared old data from 'Predictions' tab.")
    except gspread.WorksheetNotFound:
        pass
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")

    # Find the current week's games
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    current_week = 3
    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")

    for index, game in this_weeks_games.iterrows():
        home_team_full = game['Loser/tie'] if game['Unnamed: 5'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['Unnamed: 5'] == '@' else game['Loser/tie']
        home_team_abbr = full_name_to_abbr.get(home_team_full)
        away_team_abbr = full_name_to_abbr.get(away_team_full)

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")
        
        # Build the clean Game-Day Rosters for 2025
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        all_player_stats_2025 = pd.concat([dataframes['O_Player_Passing'], dataframes['O_Player_Rushing'], dataframes['O_Player_Receiving']], ignore_index=True)
        home_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)
        away_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)

        # Get Corresponding 2024 Historical Stats for Active Players
        all_player_stats_2024 = pd.concat([
            dataframes.get('2024_O_Player_Passing', pd.DataFrame()), 
            dataframes.get('2024_O_Player_Rushing', pd.DataFrame()), 
            dataframes.get('2024_O_Player_Receiving', pd.DataFrame())
        ], ignore_index=True)
        home_hist = get_historical_stats(home_roster, home_team_abbr, all_player_stats_2024)
        away_hist = get_historical_stats(away_roster, away_team_abbr, all_player_stats_2024)

        # Get Team-Level stats for both seasons
        home_team_off_2025 = dataframes['O_Team_Overall'][dataframes['O_Team_Overall']['Team_Full'] == home_team_full]
        away_team_off_2025 = dataframes['O_Team_Overall'][dataframes['O_Team_Overall']['Team_Full'] == away_team_full]
        home_team_def_2025 = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'] == home_team_full]
        away_team_def_2025 = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'] == away_team_full]
        home_team_off_2024 = dataframes.get('2024_O_Team_Overall', pd.DataFrame())
        if not home_team_off_2024.empty: home_team_off_2024 = home_team_off_2024[home_team_off_2024['Team_Full'] == home_team_full]
        away_team_off_2024 = dataframes.get('2024_O_Team_Overall', pd.DataFrame())
        if not away_team_off_2024.empty: away_team_off_2024 = away_team_off_2024[away_team_off_2024['Team_Full'] == away_team_full]

        # --- FRANCO: UPDATED PROMPT WITH NEW FORMATTING INSTRUCTIONS ---
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        Analyze the provided data for both the current (2025) and previous (2024) seasons to identify trends and player progression.

        ---
        ## {home_team_full} (Home) Team-Level Data
        - 2025 Offense: {home_team_off_2025.to_string()}
        - 2025 Defense: {home_team_def_2025.to_string()}
        - 2024 Offense: {home_team_off_2024.to_string()}

        ## {home_team_full} Active Player Stats
        - Current Season (2025): {home_roster.to_string()}
        - Previous Season (2024): {home_hist.to_string()}
        ---
        ## {away_team_full} (Away) Team-Level Data
        - 2025 Offense: {away_team_off_2025.to_string()}
        - 2025 Defense: {away_team_def_2025.to_string()}
        - 2024 Offense: {away_team_off_2024.to_string()}

        ## {away_team_full} Active Player Stats
        - Current Season (2025): {away_roster.to_string()}
        - Previous Season (2024): {away_hist.to_string()}
        ---
        Based on a comprehensive analysis of both seasons, provide the following in a clear format:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Justification:** A brief justification for your overall prediction, referencing year-over-year trends if relevant.
        4. **Key Player Stat Predictions:** For the starting QB, RB, and top WR for each team, provide predictions for their key stats. Format each player on a new line, with each stat on its own line underneath. Include a confidence percentage for each stat prediction. For example:
           CHI RB Khalil Herbert
           Rushing Yards: 75 - 80% confidence
           Receiving Yards: 15 - 60% confidence
           Touchdowns: 1 - 70% confidence
        5. **Touchdown Scorers:** List 2-3 players most likely to score a **rushing or receiving** touchdown. Do not include quarterbacks for passing touchdowns. Provide a confidence percentage from 1% to 100% for each player.
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
