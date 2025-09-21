import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import os
import pickle
from pfr_scraper import scrape_box_score # Import our new function

# --- CONFIGURATION & AUTH (UNCHANGED) ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
YEAR = 2025
# ... (get_gspread_client function is unchanged)

# --- HELPER FUNCTIONS (REWRITTEN) ---
def normalize_player_name(name):
    if not isinstance(name, str): return ""
    name = name.lower().replace('.', '').replace("'", "")
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', name).strip()
    return name

def get_out_players_set(depth_chart_df):
    out_statuses = ['O', 'IR', 'PUP', 'NFI', 'IR-R']
    out_players_df = depth_chart_df[depth_chart_df['Status'].isin(out_statuses)]
    return {normalize_player_name(name) for name in out_players_df['Player']}

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    for sheet in spreadsheet.worksheets():
        if not sheet.title.startswith("Week_"):
            try:
                sheet.hide()
                print(f"  -> Hid '{sheet.title}' sheet.")
            except Exception as e:
                # Can't hide the very first sheet, which is fine.
                if "hide the only visible sheet" not in str(e):
                    print(f"  -> Could not hide sheet '{sheet.title}': {e}")
        else:
            sheet.show() # Make sure prediction sheets are visible

# --- MAIN SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    # ... (gc, spreadsheet, and data loading is unchanged)

    # --- FRANCO: NEW TIME-AWARE LOGIC ---
    # Determine the "current week" based on the "Wednesday Rollover"
    now = datetime.now()
    schedule_df = dataframes['Schedule']
    schedule_df['datetime'] = pd.to_datetime(schedule_df['Date'] + ' ' + schedule_df['Time'], errors='coerce')
    
    # If it's after Tuesday (Wednesday is weekday 2), look ahead. Otherwise, look at the current/past week.
    if now.weekday() > 1: # 0=Mon, 1=Tue, 2=Wed
        # Find the first game that is in the future
        future_games = schedule_df[schedule_df['datetime'] > now]
        current_week = int(future_games['Week'].min()) if not future_games.empty else int(schedule_df['Week'].max())
    else:
        # Find the last game that was in the past
        past_games = schedule_df[schedule_df['datetime'] <= now]
        current_week = int(past_games['Week'].max()) if not past_games.empty else 1
    
    print(f"\nDetermined current NFL week is: {current_week}")

    # --- Get/Create the prediction sheet for the current week ---
    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Found existing sheet: '{sheet_name}'")
    except gspread.WorksheetNotFound:
        headers = [
            "Away Team", "Home Team", "Kickoff",
            "Predicted Winner", "Predicted Score", 
            "Actual Winner", "Actual Score",
            "Prediction Details"
        ]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(headers))
        worksheet.update([headers], 'A1')
        print(f"Created new sheet: '{sheet_name}'")

    # Get all games for the current week
    this_weeks_games = schedule_df[schedule_df['Week'] == str(current_week)]
    
    # ... (Configure Gemini API as before)
    
    # --- MAIN GAME LOOP (HEAVILY REWRITTEN) ---
    for index, game in this_weeks_games.iterrows():
        # ... (Get home/away teams as before)

        # Find the row for this game in our weekly sheet
        cell = worksheet.find(game['Away']) # Find away team
        if cell and worksheet.cell(cell.row, 2).value == game['Home']:
             row_num = cell.row
        else:
             # If game doesn't exist, append it and get new row number
             worksheet.append_row([game['Away'], game['Home'], game['datetime'].strftime('%Y-%m-%d %H:%M:%S')])
             row_num = len(worksheet.get_all_values())

        # --- Check game status ---
        if game['datetime'] > now:
            # GAME IS IN THE FUTURE: RUN PREDICTION
            print(f"  -> Predicting future game: {game['Away']} at {game['Home']}")
            # ... (Build game-day roster and prompt as in the last version)
            # ... (Call model.generate_content)
            
            # Parse prediction
            winner = ...
            score = ...
            details = response.text

            # Update the PREDICTED columns for this game's row
            worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
            worksheet.update(f'H{row_num}', [[details]])

        else:
            # GAME IS IN THE PAST: GET ACTUALS
            print(f"  -> Analyzing completed game: {game['Away']} at {game['Home']}")
            box_score_url = "https://www.pro-football-reference.com" + game['Boxscore']
            box_score_data = scrape_box_score(box_score_url)

            if box_score_data:
                 # Update the ACTUAL columns for this game's row
                 worksheet.update(f'F{row_num}:G{row_num}', [[game['Winner/tie'], box_score_data['final_score']]])
                 # You could expand this to format and write the actual player stats as well
                 print(f"    -> Updated actuals for {game['Away']} at {game['Home']}")
            
        time.sleep(20)

    # --- FINAL STEP: HIDE DATA SHEETS ---
    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
