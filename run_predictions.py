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

# --- FRANCO: NEW "GAME-DAY ROSTER" LOGIC ---
def get_game_day_roster(team_full_name, team_abbr, depth_chart_df, stats_df, out_players_set, pos_config):
    """
    Builds a clean roster of healthy players for a specific team based on the depth chart.
    """
    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    
    # Normalize player names in both dataframes for reliable matching
    team_depth_chart['Player_Normalized'] = team_depth_chart['Player'].apply(normalize_player_name)
    stats_df['Player_Normalized'] = stats_df['Player'].apply(normalize_player_name)
    
    active_roster_players = []
    
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players:
                break
            
            player_name_normalized = player_row['Player_Normalized']
            
            if player_name_normalized not in out_players_set:
                active_roster_players.append(player_name_normalized)
                healthy_players_found += 1
    
    # Filter the original stats DataFrame to include only the active roster players
    final_roster_df = stats_df[stats_df['Player_Normalized'].isin(active_roster_players)]
    
    return final_roster_df.drop(columns=['Player_Normalized'])


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
        full_name, abbr, injury_team_name = row['Full Name'], row['Abbreviation'], row['Injury team']
        if full_name: master_team_map[full_name] = full_name
        if abbr: master_team_map[abbr] = full_name
        if injury_team_name: master_team_map[injury_team_name] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    # Standardize All DataFrames using the Master Map
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
    current_week = 3
    this_weeks_games = schedule_df[schedule_df['Week'] == current_week].dropna(subset=['Winner/tie', 'Loser/tie'])
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")

    for index, game in this_weeks_games.iterrows():
        home_team_full = game['Loser/tie'] if game['Unnamed: 5'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['Unnamed: 5'] == '@' else game['Loser/tie']
        home_team_abbr = full_name_to_abbr.get(home_team_full)
        away_team_abbr = full_name_to_abbr.get(away_team_full)

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")
        
        # --- FRANCO: Build the clean Game-Day Rosters ---
        pos_config_passing = {'QB': 1}
        pos_config_rushing = {'RB': 2, 'QB': 1}
        pos_config_receiving = {'WR': 3, 'TE': 1}

        home_passing_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, dataframes['O_Player_Passing'], out_players_set, pos_config_passing)
        away_passing_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, dataframes['O_Player_Passing'], out_players_set, pos_config_passing)
        
        home_rushing_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, dataframes['O_Player_Rushing'], out_players_set, pos_config_rushing)
        away_rushing_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, dataframes['O_Player_Rushing'], out_players_set, pos_config_rushing)

        home_receiving_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, dataframes['O_Player_Receiving'], out_players_set, pos_config_receiving)
        away_receiving_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, dataframes['O_Player_Receiving'], out_players_set, pos_config_receiving)

        team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
        
        # --- FRANCO: FINAL PROMPT WITH UPDATED INSTRUCTIONS ---
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        Analyze the provided data, which has been filtered to show only the key players expected to be active for this game.

        ---
        ## {home_team_full} (Home) Data
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == home_team_full].to_string()}
        - Passing Offense (Active Players): {home_passing_roster.to_string()}
        - Rushing Offense (Active Players): {home_rushing_roster.to_string()}
        - Receiving Offense (Active Players): {home_receiving_roster.to_string()}

        ## {away_team_full} (Away) Data
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == away_team_full].to_string()}
        - Passing Offense (Active Players): {away_passing_roster.to_string()}
        - Rushing Offense (Active Players): {away_rushing_roster.to_string()}
        - Receiving Offense (Active Players): {away_receiving_roster.to_string()}
        ---

        Based on the structured data above, provide the following in a clear format:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Justification:** A brief justification for your overall prediction.
        4. **Key Player Stat Predictions:** Predict stats for key players expected to play (the ones listed in the data). Provide a confidence percentage from 1% to 100% for each prediction.
        5. **Touchdown Scorers:** List 2-3 players who are most likely to score a **rushing or receiving** touchdown. Do not include quarterbacks for passing touchdowns. Provide a confidence percentage from 1% to 100% for each player.
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
