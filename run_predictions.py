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
            worksheet.update('A1:F1', [['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification']])
        
        winner_match = re.search(r"Predicted Winner:\s*(.*)", prediction_text)
        score_match = re.search(r"Predicted Final Score:\s*(.*)", prediction_text)
        justification_match = re.search(r"Justification:\s*(.*)", prediction_text, re.IGNORECASE)
        winner = winner_match.group(1).strip() if winner_match else "N/A"
        score = score_match.group(1).strip() if score_match else "N/A"
        justification = justification_match.group(1).strip() if justification_match else "N/A"
        
        worksheet.append_row([week, away_team, home_team, winner, score, justification])
        print(f"  -> ✅ Prediction for {away_team} @ {home_team} written to sheet.")
    except Exception as e:
        print(f"  -> ❌ Error writing prediction to sheet: {e}")

# --- Main execution block ---
def run_predictions():
    print("Authenticating...")
    gc = get_gspread_client()
    if not gc: return
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    
    dataframes = {}
    print("\nLoading data...")
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
        
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")

    required_tabs = ['Schedule', 'FPI']
    if all(tab in dataframes for tab in required_tabs):
        
        try:
            predictions_sheet = spreadsheet.worksheet("Predictions")
            predictions_sheet.clear()
            predictions_sheet.update('A1:F1', [['Week', 'Away Team', 'Home Team', 'Predicted Winner', 'Predicted Score', 'Justification']])
        except gspread.WorksheetNotFound:
            pass

        schedule_df = dataframes['Schedule']
        schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
        
        calculation_start_date = datetime(2025, 9, 2)
        today = datetime.now()
        days_since_start = (today - calculation_start_date).days
        current_week = (days_since_start // 7) + 1
        
        this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
        
        print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
        
        for index, game in this_weeks_games.iterrows():
            home_team_full = game['Home_Team'] # Reads the clean column
            away_team_full = game['Away_Team'] # Reads the clean column

            print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

            fpi_data = dataframes['FPI']
            home_fpi = fpi_data[fpi_data['Team'].str.contains(home_team_full, case=False, na=False)]
            away_fpi = fpi_data[fpi_data['Team'].str.contains(away_team_full, case=False, na=False)]
            
            matchup_prompt = f"""
            Act as an expert NFL analyst. Predict the outcome of {away_team_full} at {home_team_full}.
            Use the provided ESPN FPI data as the primary basis for your prediction.

            ---
            HOME TEAM ({home_team_full}) FPI DATA:
            {home_fpi.to_string()}
            ---
            AWAY TEAM ({away_team_full}) FPI DATA:
            {away_fpi.to_string()}
            ---

            Provide the following:
            1. **Predicted Winner:** [Team Name]
            2. **Predicted Final Score:** [Away Score] - [Home Score]
            3. **Justification:** [Brief justification based on FPI.]
            """
            
            try:
                response = model.generate_content(matchup_prompt)
                print(response.text)
                write_prediction_to_sheet(spreadsheet, current_week, away_team_full, home_team_full, response.text)
            except Exception as e:
                print(f"Could not generate prediction: {e}")
            
            time.sleep(3)
    else:
        print(f"\n❌ Could not find all necessary data tabs. Found: {list(dataframes.keys())}")

if __name__ == "__main__":
    run_predictions()
