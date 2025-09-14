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
# The API key will be read from the GitHub secret
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Google Sheets Authentication ---
def get_gspread_client():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # Note: In an automated environment, the token.pickle must be valid.
    # The initial authorization must be done locally to generate the token.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing credentials...")
            creds.refresh(Request())
        else:
            # This part should not run in the automated script.
            # It's here for completeness but relies on the pre-generated token.pickle
            print("Credentials not found or invalid. Please re-run locally to generate token.pickle.")
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

# --- Main execution block ---
def run_main_logic():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    if not gc:
        return # Exit if authentication fails

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        return

    # Load All Data from Your Google Sheet Tabs
    dataframes = {}
    print("\nLoading and cleaning data from Google Sheet tabs...")
    try:
        for worksheet in spreadsheet.worksheets():
            title = worksheet.title
            if title.lower() in ['predictions', 'master_data']: continue
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded tab: {title}")
    except Exception as e:
        print(f"❌ Error loading sheet. Make sure the SPREADSHEET_KEY is correct: {e}")
        return
        
    # Configure the Gemini API
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        print("\n✅ Gemini API configured.")
    except Exception as e:
        print(f"❌ Error configuring Gemini API. Make sure your API Key is correct: {e}")
        return

    # --- AUTOMATED MATCHUP ANALYSIS ---
    required_tabs = ['Schedule', 'D_Overall', 'O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving', 'Injuries', 'team_match']
    if all(tab in dataframes for tab in required_tabs):
        
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
        for name, df in dataframes.items():
            team_col_found = next((col for col in df.columns if 'Tm' in col or 'Team' in col or 'Winner/tie' in col or 'Loser/tie' in col), None)
            if team_col_found:
                df['Team_Full'] = df[team_col_found].map(abbr_to_full).fillna(df[team_col_found])
                df['Team_Abbr'] = df[team_col_found].map(full_to_abbr).fillna(df[team_col_found])
                
        # Find the current week's games
        schedule_df = dataframes['Schedule']
        schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
        season_start_date = datetime(2025, 9, 4)
        today = datetime.now()
        days_since_start = (today - season_start_date).days
        current_week = (days_since_start // 7) + 1
        this_weeks_games_raw = schedule_df[schedule_df['Week'] == current_week]
        
        print(f"\nFound {len(this_weeks_games_raw) // 2} games for Week {current_week}. Starting analysis...")
        
        # DEFINITIVE LOOP TO PROCESS GAMES IN PAIRS
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

            # Prepare data
            team_defense_data = dataframes['D_Overall'][dataframes['D_Overall']['Team_Full'].isin([home_team_full, away_team_full])]
            player_passing_data = dataframes['O_Player_Passing'][dataframes['O_Player_Passing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
            player_rushing_data = dataframes['O_Player_Rushing'][dataframes['O_Player_Rushing']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
            player_receiving_data = dataframes['O_Player_Receiving'][dataframes['O_Player_Receiving']['Team_Abbr'].isin([home_team_abbr, away_team_abbr])]
            injury_data = dataframes['Injuries'][dataframes['Injuries']['Team_Full'].isin([home_team_full, away_team_full])]
            
            matchup_prompt = f"""
            Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
            Use only the data provided below.

            ---
            ## {home_team_full} (Home) Data
            - Defense: {team_defense_data[team_defense_data['Team_Full'] == home_team_full].to_string()}
            - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == home_team_abbr].to_string()}
            - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == home_team_abbr].to_string()}
            - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == home_team_abbr].to_string()}
            - Injuries: {injury_data[injury_data['Team_Full'] == home_team_full].to_string()}

            ## {away_team_full} (Away) Data
            - Defense: {team_defense_data[team_defense_data['Team_Full'] == away_team_full].to_string()}
            - Passing Offense: {player_passing_data[player_passing_data['Team_Abbr'] == away_team_abbr].to_string()}
            - Rushing Offense: {player_rushing_data[player_rushing_data['Team_Abbr'] == away_team_abbr].to_string()}
            - Receiving Offense: {player_receiving_data[player_receiving_data['Team_Abbr'] == away_team_abbr].to_string()}
            - Injuries: {injury_data[injury_data['Team_Full'] == away_team_full].to_string()}
            ---

            Based on the structured data above, provide the following in a clear format:
            1. **Predicted Winner:** [Team Name]
            2. **Predicted Final Score:** [Away Team Score] - [Home Team Score]
            3. **Key Player Stat Predictions:** Predict the Passing Yards for each QB, Rushing Yards for the lead RB on each team, and Receiving Yards for the lead WR on each team.
            4. **Touchdown Scorers:** List 2-3 players (with their full names) who are most likely to score a touchdown in this game.
            5. **Justification:** [A brief justification for your overall prediction.]
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
        print(f"\n❌ Could not find all necessary data tabs {required_tabs} to make a prediction.")

if __name__ == "__main__":
    run_main_logic()