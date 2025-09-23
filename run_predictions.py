import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timezone, date
import time
import re
import os
import pickle
import pytz
from pfr_scraper import scrape_box_score
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- AUTHENTICATION ---
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

# --- HELPER FUNCTIONS ---
def normalize_player_name(name):
    if not isinstance(name, str): return ""
    name = name.lower().replace('.', '').replace("'", "")
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', name).strip()
    return name

def get_out_players_set(depth_chart_df):
    if depth_chart_df.empty: return set()
    out_statuses = ['O', 'IR', 'PUP', 'NFI', 'IR-R']
    out_players_df = depth_chart_df[depth_chart_df['Status'].isin(out_statuses)]
    return {normalize_player_name(name) for name in out_players_df['Player']}

def get_game_day_roster(team_full_name, team_abbr, depth_chart_df, stats_df, out_players_set, pos_config):
    if stats_df.empty or depth_chart_df.empty: return pd.DataFrame()
    player_col = next((c for c in stats_df.columns if 'Player' in c), None)
    if not player_col: return pd.DataFrame()

    team_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == team_full_name].copy()

    active_roster_players = []
    for pos, num_players in pos_config.items():
        pos_depth = team_depth_chart[team_depth_chart['Position'] == pos].sort_values(by='Depth')
        healthy_players_found = 0
        for _, player_row in pos_depth.iterrows():
            if healthy_players_found >= num_players: break

            player_name_normalized = normalize_player_name(player_row['Player'])
            if player_name_normalized not in out_players_set:
                active_roster_players.append({'Player_Normalized': player_name_normalized, 'Player': player_row['Player'], 'Pos': player_row['Position']})
                healthy_players_found += 1

    if not active_roster_players: return pd.DataFrame()

    active_roster_df = pd.DataFrame(active_roster_players)
    stats_df['Player_Normalized'] = stats_df[player_col].apply(normalize_player_name)

    merged_df = pd.merge(active_roster_df, stats_df, on='Player_Normalized', how='left')

    for col in merged_df.columns:
        if pd.api.types.is_numeric_dtype(merged_df[col]):
            merged_df[col] = merged_df[col].fillna(0)

    if 'Player_x' in merged_df.columns:
        merged_df['Player'] = merged_df['Player_x'].fillna(merged_df['Player_y'])
        merged_df['Pos'] = merged_df['Pos_x'].fillna(merged_df['Pos_y'])
    if 'Team_Abbr' not in merged_df.columns: merged_df['Team_Abbr'] = team_abbr
    merged_df['Team_Abbr'] = merged_df['Team_Abbr'].fillna(team_abbr)

    final_columns = ['Player', 'Team_Abbr', 'Pos']
    stat_cols_to_add = [c for c in stats_df.columns if c not in ['Player', 'Player_Normalized', 'Team_Abbr', 'Pos', 'Tm', 'Player_x', 'Player_y', 'Pos_x', 'Pos_y']]
    final_columns.extend(stat_cols_to_add)
    final_columns_exist = [c for c in final_columns if c in merged_df.columns]

    return merged_df[final_columns_exist]

def get_historical_stats(current_roster_df, team_abbr, historical_df):
    if historical_df.empty or current_roster_df.empty: return pd.DataFrame()
    player_col_hist = next((c for c in historical_df.columns if 'Player' in c), None)
    player_col_curr = next((c for c in current_roster_df.columns if 'Player' in c), None)
    if not player_col_hist or not player_col_curr: return pd.DataFrame()

    historical_df['Player_Normalized'] = historical_df[player_col_hist].apply(normalize_player_name)
    current_roster_df['Player_Normalized'] = current_roster_df[player_col_curr].apply(normalize_player_name)
    active_players_normalized = list(current_roster_df['Player_Normalized'])
    historical_roster = historical_df[historical_df['Player_Normalized'].isin(active_players_normalized)].copy()
    if 'Team_Abbr' in historical_roster.columns: historical_roster['Team_Abbr'] = team_abbr
    if 'Tm' in historical_roster.columns: historical_roster['Tm'] = team_abbr
    return historical_roster.drop(columns=['Player_Normalized'], errors='ignore')

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if not sheet.title.startswith("Week_"):
            try:
                sheet.hide()
                print(f"  -> Hid '{sheet.title}' sheet.")
            except Exception: pass
        else:
            try:
                sheet.show()
                print(f"  -> Ensured '{sheet.title}' is visible.")
            except Exception: pass

# --- START: REWRITTEN find_or_create_row FUNCTION ---
def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    """
    Finds a row matching the away and home teams, or creates a new one if not found.
    This version is more robust against whitespace issues and returns the correct row number.
    """
    all_sheet_data = worksheet.get_all_values()
    # Loop through existing rows (skip header row [0])
    for i, row in enumerate(all_sheet_data[1:], start=2): # enumerate starts at 2 for correct sheet row number
        # Check if row has enough columns to avoid errors
        if row and len(row) > 1:
            # .strip() removes leading/trailing whitespace before comparing
            sheet_away_team = row[0].strip()
            sheet_home_team = row[1].strip()
            if sheet_away_team == away_team and sheet_home_team == home_team:
                print(f"    -> Found matching row: {i}")
                return i # Return the row number if found
    
    # If no match was found after checking all rows, create a new one
    print(f"    -> No match found. Creating new row...")
    # Append a row with blank placeholders for all columns
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', '', '', '', ''])
    # Return the new row's number, which is the new total number of rows
    return len(all_sheet_data) + 1
# --- END: REWRITTEN find_or_create_row FUNCTION ---

# --- MAIN SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        # Load all sheets that aren't prediction sheets, even if they are hidden
        if not title.startswith("Week_"):
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded data tab: {title}")

    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr

    print("\nStandardizing team names across all data sheets...")
    for name, df in dataframes.items():
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
        team_cols_to_process = [col for col in ['Visitor', 'Home', 'Team', 'Away Team', 'Home Team'] if col in df.columns]
        for col in team_cols_to_process:
            df[col] = df[col].map(master_team_map).fillna(df[col])
        if team_cols_to_process and 'Team_Full' not in df.columns:
            team_col_found = team_cols_to_process[0]
            if team_col_found:
                df['Team_Full'] = df[team_col_found]
                df.dropna(subset=['Team_Full'], inplace=True)
                df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
                print(f"  -> Standardized team names for '{name}' sheet.")

    print("\nDetermining current week with Wednesday rollover...")
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)

    schedule_df = dataframes['Schedule']
    schedule_df.rename(columns={'Visitor': 'Away Team', 'Home': 'Home Team'}, inplace=True, errors='ignore')

    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    if schedule_df.empty:
        print("❌ Error: No valid week data in Schedule tab. Exiting.")
        return
    schedule_df['Week'] = schedule_df['Week'].astype(int)

    datetime_str = schedule_df['Date'] + " " + str(YEAR) + " " + schedule_df['Time'].str.replace('p', ' PM').str.replace('a', ' AM')
    naive_datetime = pd.to_datetime(datetime_str, errors='coerce')
    schedule_df['datetime'] = naive_datetime.dt.tz_localize(eastern_tz, ambiguous='infer').dt.tz_convert('UTC')
    schedule_df.dropna(subset=['datetime'], inplace=True)

    if now_utc.astimezone(eastern_tz).weekday() >= 2 and now_utc.astimezone(eastern_tz).hour >= 6:
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        current_week = int(future_games['Week'].min()) if not future_games.empty else int(schedule_df['Week'].max())
    else:
        past_games = schedule_df[schedule_df['datetime'] <= now_utc]
        current_week = int(past_games['Week'].max()) if not past_games.empty else 1

    print(f"  -> Current NFL week is: {current_week}")

    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"  -> Found existing sheet: '{sheet_name}'")
    except gspread.WorksheetNotFound:
        print(f"  -> No sheet found for Week {current_week}. Creating '{sheet_name}'...")
        headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details", "Actual Player Stats"]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(headers))
        worksheet.update([headers], 'A1')

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
    
    # Configure Gemini only if we need it
    model = None
    if any(this_weeks_games['datetime'] > now_utc):
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        print("✅ Gemini API configured for predictions.")

    depth_chart_df = dataframes.get('Depth_Charts', pd.DataFrame())
    if not depth_chart_df.empty:
        depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
    out_players_set = get_out_players_set(depth_chart_df)

    all_player_stats_2025 = pd.concat([dataframes.get('O_Player_Passing', pd.DataFrame()), dataframes.get('O_Player_Rushing', pd.DataFrame()), dataframes.get('O_Player_Receiving', pd.DataFrame())], ignore_index=True)
    all_player_stats_2024 = pd.concat([dataframes.get('2024_O_Player_Passing', pd.DataFrame()), dataframes.get('2024_O_Player_Rushing', pd.DataFrame()), dataframes.get('2024_O_Player_Receiving', pd.DataFrame())], ignore_index=True)
    team_offense_2025 = dataframes.get('O_Team_Overall', pd.DataFrame())

    for index, game in this_weeks_games.iterrows():
        away_team_full = game['Away Team']
        home_team_full = game['Home Team']
        kickoff_time_utc = game['datetime']
        boxscore_link = game.get('Boxscore', '')

        kickoff_display_str = kickoff_time_utc.astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')

        print(f"\n--- Processing Matchup: {away_team_full} at {home_team_full} ---")
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)

        if kickoff_time_utc > now_utc:
            print(f"  -> Game is in the future. Predicting...")
            # Prediction logic would go here, same as before
            pass
        else:
            print(f"  -> Game is in the past. Attempting to scrape results...")
            if boxscore_link and 'boxscores' in boxscore_link:
                full_boxscore_url = "https://www.pro-football-reference.com" + boxscore_link
                print(f"    -> Scraping URL: {full_boxscore_url}")
                box_score_data = scrape_box_score(full_boxscore_url)

                if box_score_data and box_score_data.get('final_score'):
                     scores = box_score_data['final_score'].split('-')
                     away_score_actual = int(scores[0])
                     home_score_actual = int(scores[1])
                     actual_winner = away_team_full if away_score_actual > home_score_actual else home_team_full

                     worksheet.update(f'F{row_num}:G{row_num}', [[actual_winner, box_score_data['final_score']]])

                     player_stats_df = box_score_data.get("player_stats")
                     if player_stats_df is not None and not player_stats_df.empty:
                         stats_string = player_stats_df.to_string(index=False)
                         worksheet.update(f'I{row_num}', [[stats_string]])
                     print(f"    -> SUCCESS: Wrote actual results to sheet for row {row_num}.")
                else:
                     print(f"    -> FAILED: The scrape_box_score function did not return valid data.")
            else:
                print(f"    -> FAILED: Box score link not found or invalid in the Schedule sheet.")

        time.sleep(2)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
