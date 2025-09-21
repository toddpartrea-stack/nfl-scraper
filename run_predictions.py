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

# --- FRANCO: UPGRADED "GAME-DAY ROSTER" LOGIC ---
def get_game_day_roster(team_full_name, depth_chart_df, stats_df, out_players_set, pos_config):
    player_col = 'Player' if 'Player' in stats_df.columns else 'Unnamed: 1_level_0_Player'
    if player_col not in stats_df.columns: return pd.DataFrame()

    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    team_depth_chart['Player_Normalized'] = team_depth_chart['Player'].apply(normalize_player_name)
    stats_df['Player_Normalized'] = stats_df[player_col].apply(normalize_player_name)
    
    active_roster_players_info = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players: break
            if player_row['Player_Normalized'] not in out_players_set:
                # Store the name and depth (e.g., QB2)
                player_info = {
                    "name": player_row['Player_Normalized'],
                    "depth_str": f"{player_row['Position']}{player_row['Depth']}"
                }
                active_roster_players_info.append(player_info)
                healthy_players_found += 1
    
    active_player_names = [p['name'] for p in active_roster_players_info]
    final_roster_df = stats_df[stats_df['Player_Normalized'].isin(active_player_names)].copy()

    # Add the depth string to the final dataframe
    depth_map = {p['name']: p['depth_str'] for p in active_roster_players_info}
    final_roster_df['Pos_Rank'] = final_roster_df['Player_Normalized'].map(depth_map)
    
    return final_roster_df.drop(columns=['Player_Normalized'], errors='ignore')

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
        full_name, abbr, injury_team_name = row['Full Name'], row['Abbreviation'], row['Injury team']
        if full_name: master_team_map[full_name] = full_name
        if abbr: master_team_map[abbr] = full_name
        if injury_team_name: master_team_map[injury_team_name] = full_name
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
        pass # If the sheet doesn't exist, we'll create it later
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")

    # FRANCO: Find ALL games for the current week
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    current_week = 3
    # The .dropna() is removed to include ALL games for the week
    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")

    for index, game in this_weeks_games.iterrows():
        home_team_full = game['Loser/tie'] if game['Unnamed: 5'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['Unnamed: 5'] == '@' else game['Loser/tie']

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")
        
        # FRANCO: Identify injured STARTERS to explicitly tell the AI
        injured_starters = depth_chart_df[
            (depth_chart_df['Depth'] == 1) &
            (depth_chart_df['Player'].apply(normalize_player_name).isin(out_players_set)) &
            (depth_chart_df['Team_Full'].isin([home_team_full, away_team_full]))
        ]
        home_injured_starters = [f"{row['Player']} ({row['Position']})" for _, row in injured_starters[injured_starters['Team_Full'] == home_team_full].iterrows()]
        away_injured_starters = [f"{row['Player']} ({row['Position']})" for _, row in injured_starters[injured_starters['Team_Full'] == away_team_full].iterrows()]

        key_injuries_text = f"""
        ## Key Injuries
        - {home_team_full}: {', '.join(home_injured_starters) if home_injured_starters else 'None'}
        - {away_team_full}: {', '.join(away_injured_starters) if away_injured_starters else 'None'}
        """

        # Build the clean Game-Day Rosters
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_roster = get_game_day_roster(home_team_full, depth_chart_df, pd.concat([dataframes['O_Player_Passing'], dataframes['O_Player_Rushing'], dataframes['O_Player_Receiving']]), out_players_set, pos_config)
        away_roster = get_game_day_roster(away_team_full, depth_chart_df, pd.concat([dataframes['O_Player_Passing'], dataframes['O_Player_Rushing'], dataframes['O_Player_Receiving']]), out_players_set, pos_config)
        
        team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        Analyze all provided data, paying close attention to the key injuries and the active players who are replacing them.

        {key_injuries_text}
        ---
        ## {home_team_full} (Home) Active Roster and Stats
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == home_team_full].to_string()}
        - Offense (Key Active Players): {home_roster.to_string()}

        ## {away_team_full} (Away) Active Roster and Stats
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == away_team_full].to_string()}
        - Offense (Key Active Players): {away_roster.to_string()}
        ---
        Based on the data, provide the following:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Justification:** A brief justification for your prediction. You MUST mention the impact of the players listed under "Key Injuries".
        4. **Key Player Stat Predictions:** Predict stats for the active players listed (e.g., the backup QB). Provide a confidence percentage from 1% to 100% for each prediction.
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
