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

# --- (Authentication and write_to_sheet functions are unchanged) ---
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

def write_prediction_to_sheet(spreadsheet, week, away_team, home_team, prediction_text):
    # ... (function is unchanged)
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

# --- Main execution block ---
def run_predictions():
    # ... (Authentication, sheet opening, and data loading are the same)
    
    # --- AUTOMATED MATCHUP ANALYSIS ---
    required_tabs = ['Schedule', 'D_Overall', 'Injuries', 'team_match', 'FPI']
    if all(tab in dataframes for tab in required_tabs):
        
        # ... (Clearing old predictions and standardizing team names are the same)
        
        # ... (Finding the current week's games is the same)
        
        for index, game in this_weeks_games.iterrows():
            home_team_full = game['Home_Team']
            away_team_full = game['Away_Team']

            print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

            # --- Prepare data for the prompt, NOW INCLUDING FPI ---
            fpi_data = dataframes['FPI']
            home_fpi = fpi_data[fpi_data['TEAM'].str.contains(home_team_full, case=False, na=False)]
            away_fpi = fpi_data[fpi_data['TEAM'].str.contains(away_team_full, case=False, na=False)]
            
            team_defense_data = dataframes['D_Overall'] # Simplified for brevity
            home_def_data = team_defense_data[team_defense_data['Team'] == home_team_full]
            away_def_data = team_defense_data[team_defense_data['Team'] == away_team_full]
            
            injury_data = dataframes['Injuries'] # Simplified for brevity
            home_injury_data = injury_data[injury_data['Team'] == home_team_full]
            away_injury_data = injury_data[injury_data['Team'] == away_team_full]
            
            # --- UPDATED PROMPT ---
            matchup_prompt = f"""
            Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
            Use all the data provided below, especially the ESPN FPI, which is a strong indicator of team strength.

            ---
            ## {home_team_full} (Home) Data
            - **ESPN FPI:** {home_fpi.to_string()}
            - **Defense Stats:** {home_def_data.to_string()}
            - **Injuries:** {home_injury_data.to_string()}

            ## {away_team_full} (Away) Data
            - **ESPN FPI:** {away_fpi.to_string()}
            - **Defense Stats:** {away_def_data.to_string()}
            - **Injuries:** {away_injury_data.to_string()}
            ---

            Based on the structured data above, provide the following in a clear format:
            1. **Predicted Winner:** [Team Name]
            2. **Predicted Final Score:** [Away Team Score] - [Home Team Score]
            3. **Justification:** [A brief justification for your prediction based on the data.]
            """
            
            try:
                response = model.generate_content(matchup_prompt)
                print("--- PREDICTION (Raw Text) ---")
                print(response.text)
                
                write_prediction_to_sheet(spreadsheet, current_week, away_team_full, home_team_full, response.text)

            except Exception as e:
                print(f"Could not generate prediction for this matchup: {e}")
            
            time.sleep(10)
    else:
        print(f"\n❌ Could not find all necessary data tabs to make a prediction.")

if __name__ == "__main__":
    run_predictions()