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

# --- (Authentication and other helper functions are unchanged) ---
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
    # ... (Authentication, sheet opening, and data loading are the same)
    
    # --- Standardize Player Names across all relevant dataframes ---
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

    # ... (Rest of your working script for finding matchups)
    
    for index, game in this_weeks_games.iterrows():
        # ... (home/away team logic is the same)
        
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
        
        IMPORTANT: Pay close attention to the injury report. If a starting player (depth chart 'Depth' = 1) is injured and listed with a status of 'Out', 'IR', or 'PUP', you MUST assume they will not play. Consult the provided Depth Chart to identify their direct backup (depth chart 'Depth' = 2) and you MUST factor the skill level of the backup player into your prediction.

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
        """
        
        # ... (The rest of your prediction logic is the same)

if __name__ == "__main__":
    main()
