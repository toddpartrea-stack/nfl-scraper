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

# --- NEW: Function to standardize player names ---
def standardize_player_name(name):
    if isinstance(name, str):
        # Handle "Lastname, Firstname" format from PFR
        if ',' in name:
            parts = name.split(',')
            return f"{parts[1].strip()} {parts[0].strip()}"
        # Handle injury status suffixes like "Player Name (Q)" from depth charts
        return name.split('(')[0].strip()
    return name

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

    # --- NEW: Standardize Player Names across all relevant dataframes ---
    print("\nStandardizing player names...")
    for name, df in dataframes.items():
        player_col = next((col for col in ['Player', 'Player Name', 'Player_Info'] if col in df.columns), None)
        if player_col:
            df[player_col] = df[player_col].apply(standardize_player_name)
            print(f"  -> Standardized names for tab: {name}")

    # Check for required tabs, now including Depth_Charts
    required_tabs = ['Schedule', 'D_Overall', 'Injuries', 'team_match', 'Power_Rankings', 'Depth_Charts']
    if not all(tab in dataframes for tab in required_tabs):
        print(f"\n❌ Could not find all necessary data tabs. Found: {list(dataframes.keys())}")
        return

    # Clear old predictions
    try:
        predictions_sheet = spreadsheet.worksheet("Predictions")
        predictions_sheet.clear()
        predictions_sheet.update('A1:F1', [['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification & Player Stats']])
        print("\nCleared old data from 'Predictions' tab.")
    except gspread.WorksheetNotFound:
        pass

    # Data Standardization
    team_map_df = dataframes['team_match']
    abbr_to_full = pd.Series(team_map_df['Full Name'].values, index=team_map_df['Abbreviation']).to_dict()
    full_to_abbr = pd.Series(team_map_df['Abbreviation'].values, index=team_map_df['Full Name']).to_dict()
    injury_to_full = pd.Series(team_map_df['Full Name'].values, index=team_map_df['Injury team']).to_dict()

    for name, df in dataframes.items():
        if name == 'Injuries' and 'Team' in df.columns:
            df['Team_Full'] = df['Team'].map(injury_to_full)
        else:
            team_col_found = next((col for col in df.columns if 'Tm' in col or 'Team' in col or 'Winner/tie' in col or 'Loser/tie' in col), None)
            if team_col_found:
                df['Team_Full'] = df[team_col_found].map(abbr_to_full).fillna(df[team_col_found])
        
        if 'Team_Full' in df.columns:
            df['Team_Abbr'] = df['Team_Full'].map(full_to_abbr)


    # Find the current week's games (Your working logic)
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    calculation_start_date = datetime(2025, 9, 2)
    today = datetime.now()
    days_since_start = (today - calculation_start_date).days
    current_week = (days_since_start // 7) + 1
    this_weeks_games_raw = schedule_df[schedule_df['Week'] == current_week]
    
    print(f"\nFound {len(this_weeks_games_raw) // 2} games for Week {current_week}. Starting analysis...")
    
    # Process games in pairs (Your working logic)
    for i in range(0, len(this_weeks_games_raw), 2):
        row1 = this_weeks_games_raw.iloc[i]
        row2 = this_weeks_games_raw.iloc[i+1]
        if pd.isna(row1['Winner/tie']) or pd.isna(row2['Winner/tie']):
            continue

        if row1['Unnamed: 5'] == '@':
            home_team_full = row2['Winner/tie']
            away_team_full = row1['Winner/tie']
        else:
            home_team_full = row1['Winner/tie']
            away_team_full = row2['Winner/tie']
            
        home_team_abbr = full_to_abbr.get(home_team_full)
        away_team_abbr = full_to_abbr.get(away_team_full)

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

        # Prepare all data for the prompt
        power_rankings_data = dataframes['Power_Rankings']
        home_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(home_team_full, case=False, na=False)]
        away_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(away_team_full, case=False, na=False)]
        
        injury_data = dataframes['Injuries']
        home_injury_data = injury_data[injury_data['Team_Full'] == home_team_full]
        away_injury_data = injury_data[injury_data['Team_Full'] == away_team_full]
        
        depth_chart_data = dataframes['Depth_Charts']
        home_depth_chart = depth_chart_data[depth_chart_data['Team_Abbr'] == home_team_abbr]
        away_depth_chart = depth_chart_data[depth_chart_data['Team_Abbr'] == away_team_abbr]
        
        # --- The Final, Most Advanced Prompt ---
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        
        You MUST follow these steps in your analysis:
        1. Review the 'Injuries' data for each team.
        2. If a key player is listed with a status of 'IR', 'Out', or 'Doubtful', find that player in the 'Depth Chart' data.
        3. Identify the player listed directly below them in the depth chart as their replacement.
        4. Factor the absence of the starter and the presence of the backup into your final predictions for player stats and the overall game outcome.
        5. In your 'Justification', you MUST mention the specific impact of key injuries.

        ---
        ## {home_team_full} (Home) Data
        - Power Ranking: {home_power_rankings.to_string()}
        - Injuries: {home_injury_data.to_string()}
        - Depth Chart: {home_depth_chart.to_string()}

        ## {away_team_full} (Away) Data
        - Power Ranking: {away_power_rankings.to_string()}
        - Injuries: {away_injury_data.to_string()}
        - Depth Chart: {away_depth_chart.to_string()}
        ---

        Based on your analysis of all the structured data above, provide the following:
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Outcome Confidence Percentage: [Provide a confidence percentage from 1% to 100% for the predicted winner.
        3. **Key Player Stat Predictions:** Predict the Passing Yards and Rushing Yards for each QB. Predict Rushing Yards for the lead RB on each team. Predict Receiving Yards for the lead WR on each team. Include your confidence percentage of each player achieving the predictions
        4. **Touchdown Scorers:** List 2-3 players (by name and position, e.g., 'RB Name (RB)') who are most likely to score a rushing or receiving touchdown in this game. Do not include QBs for passing touchdowns. Provide a confidence percentage from 1% to 100% for each touchdown scorer.
        5. **Justification:** A brief justification for your overall prediction including the most important deciding factors for your prediction.
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
