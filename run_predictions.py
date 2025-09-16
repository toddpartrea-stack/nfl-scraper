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

SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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

def write_prediction_to_sheet(spreadsheet, week, away_team, home_team, prediction_text):
    # ... (function is unchanged)

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

    required_tabs = ['Schedule']
    if all(tab in dataframes for tab in required_tabs):
        
        # ... (Clearing old predictions is the same)
        
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

            # ... (The rest of the prediction logic is the same)
    else:
        print(f"\n❌ Could not find all necessary data tabs. Found: {list(dataframes.keys())}")

if __name__ == "__main__":
    run_predictions()