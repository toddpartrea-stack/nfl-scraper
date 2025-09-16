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
    # ... (function is unchanged)

# --- Main execution block ---
def run_predictions():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    if not gc: return

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        return

    # --- CORRECTED: Initialize dataframes dictionary ---
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
        # The script will continue but likely fail the next check
        
    # Configure Gemini API
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        print("\n✅ Gemini API configured.")
    except Exception as e:
        print(f"❌ Error configuring Gemini API: {e}")
        return

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