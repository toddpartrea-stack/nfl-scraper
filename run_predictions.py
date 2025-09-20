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
        if ',' in name:
            parts = name.split(',')
            return f"{parts[1].strip()} {parts[0].strip()}"
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

    # Standardize Player Names across all relevant dataframes
    print("\nStandardizing player names...")
    for name, df in dataframes.items():
        player_col = next((col for col in ['Player', 'Player Name', 'Player_Info'] if col in df.columns), None)
        if player_col:
            df[player_col] = df[player_col].apply(standardize_player_name)
            print(f"  -> Standardized names for tab: {name}")

    # Check for required tabs
    required_tabs = ['Schedule', 'Power_Rankings', 'Injuries', 'Depth_Charts']
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

    # Find the current week's games
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    calculation_start_date = datetime(2025, 9, 2)
    today = datetime.now()
    days_since_start = (today - calculation_start_date).days
    current_week = (days_since_start // 7) + 1
    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
    
    for index, game in this_weeks_games.iterrows():
        home_team_full = game['Home_Team']
        away_team_full = game['Away_Team']

        print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

        # Prepare all data for the prompt
        power_rankings_data = dataframes['Power_Rankings']
        home_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(home_team_full, case=False, na=False)]
        away_power_rankings = power_rankings_data[power_rankings_data['Team'].str.contains(away_team_full, case=False, na=False)]
        
        injury_data = dataframes['Injuries']
        home_injury_data = injury_data[injury_data['Team'] == home_team_full]
        away_injury_data = injury_data[injury_data['Team'] == away_team_full]
        
        depth_chart_data = dataframes['Depth_Charts']
        home_depth_chart = depth_chart_data[depth_chart_data['Team'] == home_team_full]
        away_depth_chart = depth_chart_data[depth_chart_data['Team'] == away_team_full]
        
        # --- The Final, Most Advanced Prompt ---
        matchup_prompt = f"""
        Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
        
        IMPORTANT: Pay close attention to the injury report. If a starting player (depth chart 'Depth' = 1) is on the injury list with a status of 'Out', 'IR', or 'PUP', you MUST assume they will not play. Consult the provided Depth Chart to identify their direct backup (depth chart 'Depth' = 2) and you MUST factor the skill level of the backup player into your prediction.

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

        Based on all the structured data above, provide your final, most informed prediction.
        1. **Game Prediction:** Predicted Winner and Predicted Final Score.
        2. **Justification:** A brief justification for your prediction, mentioning the impact of any key injuries and their replacements.
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
