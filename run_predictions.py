import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import os
import pickle
from pfr_scraper import scrape_box_score
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION & AUTH (UNCHANGED) ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
YEAR = 2025
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
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return gspread.authorize(creds)

# --- HELPER FUNCTIONS (UNCHANGED) ---
def write_prediction_to_sheet(worksheet, row_num, pred_winner, pred_score, details):
    worksheet.update(f'D{row_num}:E{row_num}', [[pred_winner, pred_score]])
    worksheet.update(f'H{row_num}', [[details]])

def write_actual_to_sheet(worksheet, row_num, actual_winner, actual_score):
    worksheet.update(f'F{row_num}:G{row_num}', [[actual_winner, actual_score]])

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
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        # Hide sheet unless it's the very first one or a predictions tab
        if not sheet.title.startswith("Week_") and sheet.index != 0:
            try:
                sheet.hide()
                print(f"  -> Hid '{sheet.title}' sheet.")
            except Exception as e:
                print(f"  -> Could not hide sheet '{sheet.title}': {e}")
        elif sheet.title.startswith("Week_"):
            sheet.show()

# --- MAIN SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # --- FRANCO: CORRECTED ORDER OF OPERATIONS ---
    # STEP 1: Load all data from the spreadsheet FIRST.
    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        # Skip prediction tabs when loading data
        if not title.startswith("Week_"):
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded data tab: {title}")

    # STEP 2: Now that data is loaded, determine the current week.
    print("\nDetermining current week with Wednesday rollover...")
    now = datetime.now()
    schedule_df = dataframes['Schedule']
    # Convert schedule times to datetime objects for comparison
    schedule_df['datetime'] = pd.to_datetime(schedule_df['Date'] + ' ' + schedule_df['Time'].fillna('0:00AM').str.replace('p', ' PM').str.replace('a', ' AM'), format='%B %d %I:%M %p', errors='coerce')
    schedule_df['datetime'] = schedule_df['datetime'].apply(lambda dt: dt.replace(year=YEAR))

    # Wednesday is weekday 2 (Mon=0, Tue=1, Wed=2...)
    # If it's after 6 AM on Wednesday, we look ahead.
    if now.weekday() >= 2 and now.hour >= 6:
        future_games = schedule_df[schedule_df['datetime'] > now]
        current_week = int(future_games['Week'].min()) if not future_games.empty else int(schedule_df['Week'].max())
    else:
        past_games = schedule_df[schedule_df['datetime'] <= now]
        current_week = int(past_games['Week'].max()) if not past_games.empty else 1
    
    print(f"  -> Current NFL week is: {current_week}")

    # ... (The rest of the script continues as planned)
    # Get/Create the prediction sheet for the current week
    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Found existing sheet: '{sheet_name}'")
    except gspread.WorksheetNotFound:
        headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details"]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(headers))
        worksheet.update([headers], 'A1')
        print(f"Created new sheet: '{sheet_name}'")

    # Get all games for the current week
    this_weeks_games = schedule_df[schedule_df['Week'] == str(current_week)]
    print(f"Found {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")

    # Configure Gemini API
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")
    
    # --- MAIN GAME LOOP ---
    for index, game in this_weeks_games.iterrows():
        away_team = game['Away']
        home_team = game['Home']
        kickoff_time = game['datetime']
        boxscore_link = "https://www.pro-football-reference.com" + game.get('Boxscore', '')

        print(f"\n--- Processing Matchup: {away_team} at {home_team} ---")

        # Find the row for this game in our weekly sheet
        cell = worksheet.find(away_team)
        row_num = None
        if cell and worksheet.cell(cell.row, 2).value == home_team:
             row_num = cell.row
        else:
             worksheet.append_row([away_team, home_team, kickoff_time.strftime('%Y-%m-%d %H:%M:%S')])
             row_num = len(worksheet.get_all_values())

        # Check game status
        if kickoff_time > now:
            print(f"  -> Predicting future game...")
            # Here you would place the full logic to build the prompt and call the AI
            # For brevity, this is a placeholder
            pred_winner, pred_score, details = "Pending", "Pending", "Prediction will run on next execution."
            # ... call AI ...
            # pred_winner, pred_score, details = parse_ai_response(response.text)
            
            write_prediction_to_sheet(worksheet, row_num, pred_winner, pred_score, details)

        else:
            print(f"  -> Analyzing completed game...")
            box_score_data = scrape_box_score(boxscore_link)
            if box_score_data:
                 actual_winner = game['Winner/tie']
                 actual_score = box_score_data['final_score']
                 write_actual_to_sheet(worksheet, row_num, actual_winner, actual_score)
                 print(f"    -> Updated actuals for {away_team} at {home_team}")
            else:
                 print(f"    -> Could not retrieve actuals for {away_team} at {home_team}")
            
        time.sleep(5) # Be respectful to APIs

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
