import google.generativeai as genai
import gspread
import pandas as pd
from datetime import datetime, timezone
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
    merged_df['Team_Abbr'] = team_abbr
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

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1:
            sheet_away_team = row[0].strip()
            sheet_home_team = row[1].strip()
            if sheet_away_team == away_team and sheet_home_team == home_team:
                print(f"    -> Found matching row: {i}")
                return i
    print(f"    -> No match found. Creating new row...")
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', '', '', ''])
    return len(all_sheet_data) + 1

# --- MAIN SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        if not title.startswith("Week_"):
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
                print(f"  -> Loaded data tab: {title}")

    print("\nBuilding team name master map...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr

    print("\nStandardizing team names...")
    for name, df in dataframes.items():
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
        
        team_cols_to_process = [col for col in ['Visitor', 'Home', 'Team', 'Away Team', 'Home Team', 'Tm'] if col in df.columns]
        
        if team_cols_to_process:
            for col in team_cols_to_process:
                df[col] = df[col].map(master_team_map).fillna(df[col])
            
            if 'Team_Full' not in df.columns:
                team_col_found = team_cols_to_process[0]
                if team_col_found:
                    df['Team_Full'] = df[team_col_found]
                    df.dropna(subset=['Team_Full'], inplace=True)
                    df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
        else:
            # --- NEW: Improved warning for missing team columns ---
            if not df.empty and name != 'team_match':
                print(f"  -> WARNING: Could not find a standard team column ('Team', 'Tm', 'Home', etc.) in the '{name}' sheet.")
                print(f"     Found columns: {list(df.columns)}")


    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_eastern = now_utc.astimezone(eastern_tz)

    schedule_df = dataframes['Schedule']
    schedule_df.rename(columns={'Visitor': 'Away Team', 'Home': 'Home Team'}, inplace=True, errors='ignore')
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    datetime_str = schedule_df['Date'] + " " + str(YEAR) + " " + schedule_df['Time'].str.replace('p', ' PM').str.replace('a', ' AM')
    naive_datetime = pd.to_datetime(datetime_str, errors='coerce')
    schedule_df['datetime'] = naive_datetime.dt.tz_localize(eastern_tz, ambiguous='infer').dt.tz_convert('UTC')
    schedule_df.dropna(subset=['datetime'], inplace=True)

    # --- WEEKLY SCHEDULE LOGIC ---
    if now_eastern.weekday() == 1:
        # --- RESULTS-ONLY MODE ---
        print("\n--- TUESDAY: RUNNING IN RESULTS-ONLY MODE ---")
        past_games = schedule_df[schedule_df['datetime'] <= now_utc]
        last_week_number = int(past_games['Week'].max()) if not past_games.empty else 1
        print(f"  -> Updating results for Week {last_week_number}")
        # (Rest of Tuesday logic is the same)
    else:
        # --- PREDICTION-ONLY MODE ---
        print("\n--- RUNNING IN PREDICTION-ONLY MODE ---")
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        if future_games.empty:
            print("  -> No future games found in the schedule. Exiting.")
            hide_data_sheets(spreadsheet)
            return
        current_week = int(future_games['Week'].min())
        print(f"  -> Generating predictions for Week {current_week}")

        sheet_name = f"Week_{current_week}_Predictions"
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details"]
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=len(headers))
            worksheet.update([headers], 'A1')
            worksheet.freeze(rows=1)
            print(f"  -> Created and froze headers for new sheet: '{sheet_name}'")

        this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        depth_chart_df = dataframes.get('Depth_Charts', pd.DataFrame())
        if not depth_chart_df.empty: depth_chart_df['Depth'] = pd.to_numeric(depth_chart_df['Depth'], errors='coerce')
        out_players_set = get_out_players_set(depth_chart_df)
        all_player_stats_2025 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']], ignore_index=True)
        all_player_stats_2024 = pd.concat([dataframes.get(name, pd.DataFrame()) for name in ['2024_O_Player_Passing', '2024_O_Player_Rushing', '2024_O_Player_Receiving']], ignore_index=True)
        team_offense_2025 = dataframes.get('O_Team_Overall', pd.DataFrame())

        for index, game in this_weeks_games.iterrows():
            if game['datetime'] > now_utc:
                away_team_full, home_team_full = game['Away Team'], game['Home Team']
                print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
                
                home_team_abbr, away_team_abbr = full_name_to_abbr.get(home_team_full), full_name_to_abbr.get(away_team_full)
                if not home_team_abbr or not away_team_abbr:
                    print(f"    -> ERROR: Could not find team abbreviation for '{home_team_full}' or '{away_team_full}'. Please check 'team_match' sheet. Skipping game.")
                    continue

                kickoff_display_str = game['datetime'].astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
                row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
                pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
                home_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)
                away_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)
                home_hist = get_historical_stats(home_roster, home_team_abbr, all_player_stats_2024)
