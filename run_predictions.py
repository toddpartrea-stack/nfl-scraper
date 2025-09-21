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

# --- FRANCO: FINAL VERSION OF "GAME-DAY ROSTER" LOGIC ---
def get_game_day_roster(team_full_name, team_abbr, depth_chart_df, stats_df, out_players_set, pos_config):
    player_col = next((c for c in ['Player', 'Unnamed: 1_level_0_Player'] if c in stats_df.columns), None)
    if not player_col:
        print(f"ERROR: Could not find any player column in a stats sheet. Columns are: {stats_df.columns.tolist()}")
        return pd.DataFrame()

    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()
    
    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players: break
            
            player_name_normalized = normalize_player_name(player_row['Player'])
            if player_name_normalized not in out_players_set:
                active_roster_players.append({
                    'Player_Normalized': player_name_normalized,
                    'Player': player_row['Player'],
                    'Pos': player_row['Position']
                })
                healthy_players_found += 1

    if not active_roster_players:
        return pd.DataFrame()

    active_roster_df = pd.DataFrame(active_roster_players)
    stats_df['Player_Normalized'] = stats_df[player_col].apply(normalize_player_name)
    
    merged_df = pd.merge(active_roster_df, stats_df, on='Player_Normalized', how='left')

    # For players who have no stats (e.g., rookies), fill missing numerical data with 0
    for col in merged_df.columns:
        if pd.api.types.is_numeric_dtype(merged_df[col]):
            merged_df[col] = merged_df[col].fillna(0)
    
    # Fill missing text data with reasonable defaults
    merged_df['Player'] = merged_df['Player_x'].fillna(merged_df['Player_y'])
    merged_df['Pos'] = merged_df['Pos_x'].fillna(merged_df['Pos_y'])
    # Use the standardized 'Team_Abbr' column
    if 'Team_Abbr' not in merged_df.columns:
        merged_df['Team_Abbr'] = team_abbr
    merged_df['Team_Abbr'] = merged_df['Team_Abbr'].fillna(team_abbr)

    # Select and reorder columns for a clean output
    final_columns = ['Player', 'Team_Abbr', 'Pos']
    stat_cols_to_add = [c for c in stats_df.columns if c not in ['Player', 'Player_Normalized', 'Team_Abbr', 'Pos', 'Tm']]
    final_columns.extend(stat_cols_to_add)
    final_columns_exist = [c for c in final_columns if c in merged_df.columns]
    
    return merged_df[final_columns_exist]

# --- Main execution block ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        data = worksheet.get_all_values()
        if data:
            dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
            print(f"  -> Loaded tab: {title}")
    
    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    print("\nStandardizing team names across all data sheets...")
    possible_team_cols = ['Tm', 'Team', 'Winner/tie', 'Loser/tie', 'Unnamed: 1_level_0_Tm', 'Unnamed: 3_level_0_Team']
    for name, df in dataframes.items():
        team_col_found = next((col for col in possible_team_cols if col in df.columns), None)
        if team_col_found:
            df['Team_Full'] = df[team_col_found].map(master_team_map)
            df.dropna(subset=['Team_Full'], inplace=True)
            df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
            print(f"  -> Standardized team names for '{name}' sheet.")

    depth_chart_df = dataframes['Depth_Charts']
    depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
    out_players_set = get_out_players_set(depth_chart_df)
    
    try:
        predictions_sheet = spreadsheet.worksheet("Predictions")
        predictions_sheet.clear()
        predictions_sheet.update(range_name='A1:F1', values=[['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])
        print("\nCleared old data from 'Predictions' tab.")
    except gspread.WorksheetNotFound: pass
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")

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
        
        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        all_player_stats = pd.concat([dataframes['O_Player_Passing'], dataframes['O_Player_Rushing'], dataframes['O_Player_Receiving']], ignore_index=True)
        home_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, all_player_stats, out_players_set, pos_config)
        away_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, all_player_stats, out_players_set, pos_config)
        
        team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
        
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        The data provided shows the key active players for this game. If a player has all zero stats, it means they are likely a rookie or have not recorded stats this season.

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
        3. **Justification:** A brief justification for your prediction.
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
