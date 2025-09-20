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

# --- Google Sheets Authentication (Your working version) ---
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

# --- Function to write predictions to the sheet (Your working version) ---
def write_prediction_to_sheet(spreadsheet, week, away_team, home_team, prediction_text):
    try:
        sheet_name = "Predictions"
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=6)
            worksheet.update('A1:F1', [['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])

        justification = prediction_text.strip()
        winner_match = re.search(r"Predicted Winner:\s*(.*)", prediction_text)
        score_match = re.search(r"Predicted Final Score:\s*(.*)", prediction_text)
        winner = winner_match.group(1).strip() if winner_match else "See Justification"
        score = score_match.group(1).strip() if score_match else "See Justification"

        worksheet.append_row([week, away_team, home_team, winner, score, justification])
        print(f"  -> ✅ Prediction for {away_team} @ {home_team} written to sheet.")
    except Exception as e:
        print(f"  -> ❌ Error writing prediction to sheet: {e}")

# --- FRANCO: NEW FUNCTION TO IDENTIFY INJURED PLAYERS ---
def get_injured_players_set(injuries_df):
    """
    Reads the Injuries DataFrame, identifies players who will not play based on keywords
    in their status, and returns a set of their names for fast filtering.
    """
    # Based on your scraper, the player name and status columns are 'Player' and 'Game Status'
    PLAYER_NAME_COLUMN = 'Player'
    STATUS_COLUMN = 'Game Status'

    if PLAYER_NAME_COLUMN not in injuries_df.columns or STATUS_COLUMN not in injuries_df.columns:
        print(f"Warning: Could not find '{PLAYER_NAME_COLUMN}' or '{STATUS_COLUMN}' in the Injuries tab. Skipping injury filter.")
        return set()

    # The confirmed list of keywords that mean a player is out.
    OUT_KEYWORDS = [
        'IR', 
        'Injured Reserve', 
        'Out', 
        'Physically Unable to Perform', 
        'PUP', 
        'NFI'
    ]

    # Combine keywords into a search pattern: 'IR|Out|PUP|etc.'
    search_pattern = '|'.join(OUT_KEYWORDS)
    
    # Ensure the status column is a string to prevent errors with .str accessor
    injuries_df[STATUS_COLUMN] = injuries_df[STATUS_COLUMN].astype(str)

    # Filter the DataFrame to find rows where the status contains any of our keywords (case-insensitive)
    injured_subset_df = injuries_df[injuries_df[STATUS_COLUMN].str.contains(search_pattern, case=False, na=False)]

    # Create the final set of player names. A set is used for very fast lookups.
    injured_players = set(injured_subset_df[PLAYER_NAME_COLUMN])
    
    print(f"Identified {len(injured_players)} players who are out.")
    return injured_players

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
    print("\nLoading and cleaning data from Google Sheet tabs...")
    try:
        for worksheet in spreadsheet.worksheets():
            title = worksheet.title
            if title.lower() in ['predictions']: continue
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded tab: {title}")
    except Exception as e:
        print(f"❌ Error loading sheet: {e}")
        return
        
    # Configure Gemini API
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        print("\n✅ Gemini API configured.")
    except Exception as e:
        print(f"❌ Error configuring Gemini API: {e}")
        return

    # Check for required tabs
    required_tabs = ['Schedule', 'D_Overall', 'O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving', 'Injuries', 'team_match', 'Power_Rankings']
    if not all(tab in dataframes for tab in required_tabs):
        print(f"\n❌ Could not find all necessary data tabs. Found: {list(dataframes.keys())}")
        return
        
    # --- FRANCO: GET THE SET OF INJURED PLAYERS ---
    print("\nFiltering out injured players from offensive stats...")
    injured_players_set = get_injured_players_set(dataframes['Injuries'])

    # --- FRANCO: REMOVE INJURED PLAYERS FROM OFFENSIVE DATAFRAMES ---
    # We use the `~` symbol, which means NOT, to keep only the players NOT IN the injured set.
    for stat_sheet_name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']:
        df = dataframes[stat_sheet_name]
        if 'Player' in df.columns:
            initial_rows = len(df)
            # The 'Player' column in PFR data often has special characters like '*' or '+' which we remove.
            cleaned_player_col = df['Player'].str.replace(r'[*+]', '', regex=True).str.strip()
            df = df[~cleaned_player_col.isin(injured_players_set)]
            
            dataframes[stat_sheet_name] = df # Update the dictionary with the filtered dataframe
            filtered_rows = len(df)
            print(f"  -> Removed {initial_rows - filtered_rows} injured players from {stat_sheet_name}.")
        else:
            print(f"Warning: 'Player' column not found in '{stat_sheet_name}', cannot filter.")

    # Clear old predictions
    try:
        predictions_sheet = spreadsheet.worksheet("Predictions")
        predictions_sheet.clear()
        predictions_sheet.update('A1:F1', [['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])
        print("\nCleared old data from 'Predictions' tab.")
    except gspread.WorksheetNotFound:
        pass

    # Data Standardization (Your working logic)
    team_map_df = dataframes['team_match']
    abbr_to_full = pd.Series(team_map_df['Full Name'].values, index=team_map_df['Abbreviation']).to_dict()
    full_to_abbr = pd.Series(team_map_df['Abbreviation'].values, index=team_map_df['Full Name']).to_dict()
    possible_team_cols = ['Tm', 'Team', 'Winner/tie', 'Loser/tie', 'Unnamed: 1_level_0_Tm', 'Unnamed: 3_level_0_Team']
    for name, df in dataframes.items():
        team_col_found = next((col for col in possible_team_cols if col in df.columns), None)
        if team_col_found:
            df['Team_Full'] = df[team_col_found].map(abbr_to_full).fillna(df[team_col_found])
            df['Team_Abbr'] = df[team_col_found].map(full_to_abbr).fillna(df[team_col_found])

    # Find the current week's games (Your working logic)
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    calculation_start_date = datetime(2025, 9, 2)
    today = datetime.now()
    days_since_start = (today - calculation_start_date).days
    current_week = (days_since_start // 7) + 1
    this_weeks_games = schedule_df[schedule_df['Week'] == current_week].dropna(subset=['Winner/tie', 'Loser/tie'])
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
    
    for index, game in this_weeks_games.iterrows():
        # Matchup logic from your working script
        home_team_full = game['Loser/tie'] if game['Unnamed: 5'] == '@' else game['Winner/tie']
        away_team_full = game['Winner/tie'] if game['Unnamed: 5'] == '@' else game['Loser/tie']
        home_team_abbr = full_to_abbr.get(home_team_full)
        away_team_abbr = full_to_abbr.get(away_team_full)

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

        # Prepare all data for the prompt, which now uses the FILTERED dataframes
        power_rankings_data = dataframes['Power_Rankings']
        home_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(home_team_full, case=False, na=False)]
        away_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(away_team_full, case=False, na=False)]
        
        team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
        # These now pull from the dataframes that have had injured players removed.
        player_passing_data = dataframes['O_Player_Passing'][dataframes['O_Player_Passing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        player_rushing_data = dataframes['O_Player_Rushing'][dataframes['O_Player_Rushing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        player_receiving_data = dataframes['O_Player_Receiving'][dataframes['O_Player_Receiving']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
        # We still send the raw injury data so the AI knows who is out.
        injury_data = dataframes['Injuries'][dataframes['Injuries']['Team'].isin([home_team_full, away_team_full])]
        
        # Updated prompt - no changes needed here as the data is already clean
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        Use all of the data provided to make the most informed decision. 
        IMPORTANT: Pay close attention to the 'Injuries' data 
        1. if a starting player (depth chart 'Depth' = 1) has an Injury Status with any of the key words IR, 
        Injured Reserve, Out, PUP, Physically Unable to Perform, or NFI you MUST assume they will not play. 
        2. Consult the provided Depth Chart to identify their direct backup (depth chart 'Depth' = 2) and you MUST factor the skill level of the 
        backup player into your prediction.  

        ---
        ## {home_team_full} (Home) Data
        - Power Ranking: {home_power_rankings.to_string()}
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == home_team_full].to_string()}
        - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == home_team_abbr].to_string()}
        - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == home_team_abbr].to_string()}
        - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == home_team_abbr].to_string()}
        - Injuries: {injury_data[injury_data['Team'] == home_team_full].to_string()}

        ## {away_team_full} (Away) Data
        - Power Ranking: {away_power_rankings.to_string()}
        - Defense: {team_defense_data[team_defense_data['Team_Full'] == away_team_full].to_string()}
        - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == away_team_abbr].to_string()}
        - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == away_team_abbr].to_string()}
        - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == away_team_abbr].to_string()}
        - Injuries: {injury_data[injury_data['Team'] == away_team_full].to_string()}
        ---

        Based on the structured data above, provide the following in a clear format:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
        3. **Key Player Stat Predictions:** Predict the Passing Yards and Rushing Yards for each QB. Predict Rushing Yards for the lead RB on each team. Predict Receiving Yards for the lead WR on each team. Include your confidence percentage of each player achieving the predictions.
        4. **Touchdown Scorers:** List 2-3 players (by name and position, e.g., 'RB Name (RB)') who are most likely to score a rushing or receiving touchdown in this game. Do not include QBs for passing touchdowns. Provide a confidence percentage for each touchdown scorer.
        5. **Justification:** A brief justification for your overall prediction including the most important deciding factors for your prediction. Including any key players that will not play
        """
        
        try:
            response = model.generate_content(matchup_prompt)
            print("--- PREDICTION (Raw Text) ---")
            print(response.text)
            
            write_prediction_to_sheet(spreadsheet, current_week, away_team_full, home_team_full, response.text)

        except Exception as e:
            print(f"Could not generate prediction for this matchup: {e}")
        
        time.sleep(10)

if __name__ == "__main__":
    main()
