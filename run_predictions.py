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

# --- AUTHENTICATION & HELPERS ---
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
    for sheet in sheets[1:]:
        if not sheet.title.startswith("Week_"):
            try:
                sheet.hide()
                print(f"  -> Hid '{sheet.title}' sheet.")
            except Exception: pass
        else:
            try:
                sheet.show()
            except Exception: pass

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1 and row[0] == away_team and row[1] == home_team:
            return i
    worksheet.append_row([away_team, home_team, kickoff_str])
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

    print("\nBuilding team name master map from 'team_match' sheet...")
    team_map_df = dataframes['team_match']
    master_team_map, full_name_to_abbr = {}, {}
    for _, row in team_map_df.iterrows():
        full_name, abbr = row['Full Name'], row['Abbreviation']
        for col in team_map_df.columns:
            if row[col]: master_team_map[row[col]] = full_name
        if full_name and abbr: full_name_to_abbr[full_name] = abbr
    
    print("\nStandardizing team names across all data sheets...")
    possible_team_cols = ['Away Team', 'Home Team', 'Team']
    for name, df in dataframes.items():
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
        
        for col in possible_team_cols:
            if col in df.columns:
                df[col] = df[col].map(master_team_map).fillna(df[col])
        
        team_col_found = next((col for col in possible_team_cols if col in df.columns), None)
        if team_col_found and 'Team_Full' not in df.columns:
            df['Team_Full'] = df[team_col_found]
            df.dropna(subset=['Team_Full'], inplace=True)
            df['Team_Abbr'] = df['Team_Full'].map(full_name_to_abbr)
            print(f"  -> Standardized team names for '{name}' sheet.")

    print("\nDetermining current week with Wednesday rollover...")
    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    
    schedule_df = dataframes['Schedule']
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    if schedule_df.empty:
        print("Error: No valid week data in Schedule tab. Exiting.")
        return
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    
    datetime_str = schedule_df['Date'] + " " + str(YEAR) + " " + schedule_df['Time'].str.replace('p', ' PM').str.replace('a', ' AM')
    schedule_df['datetime_local'] = pd.to_datetime(datetime_str, format='%B %d %Y %I:%M %p', errors='coerce')
    schedule_df['datetime'] = schedule_df['datetime_local'].apply(lambda dt: eastern_tz.localize(dt, is_dst=None) if pd.notnull(dt) else pd.NaT)
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
    except gspread.WorksheetNotFound:
        headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Actual Winner", "Actual Score", "Prediction Details", "Actual Player Stats"]
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(headers))
        worksheet.update([headers], 'A1')

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    print(f"\nFound {len(this_weeks_games)} games for Week {current_week}. Starting analysis...")
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("\n✅ Gemini API configured.")
    
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
        kickoff_time = game['datetime']
        boxscore_link = game.get('Boxscore', '')
        
        print(f"\n--- Processing Matchup: {away_team_full} at {home_team_full} ---")
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

        if kickoff_time > now_utc:
            print(f"  -> Predicting future game...")
            home_team_abbr = full_name_to_abbr.get(home_team_full)
            away_team_abbr = full_name_to_abbr.get(away_team_full)
            pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
            home_roster = get_game_day_roster(home_team_full, home_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)
            away_roster = get_game_day_roster(away_team_full, away_team_abbr, depth_chart_df, all_player_stats_2025, out_players_set, pos_config)
            home_hist = get_historical_stats(home_roster, home_team_abbr, all_player_stats_2024)
            away_hist = get_historical_stats(away_roster, away_team_abbr, all_player_stats_2024)
            
            home_team_off_2025 = team_offense_2025[team_offense_2025['Team_Full'] == home_team_full]
            away_team_off_2025 = team_offense_2025[team_offense_2025['Team_Full'] == away_team_full]
            
            matchup_prompt = f"""
            Act as an expert NFL analyst. Predict the outcome of the {away_team_full} at {home_team_full} game.
            Analyze the provided data. If a player has all zero stats, it means they are a rookie or have not recorded stats this season.

            ---
            ## {home_team_full} (Home) Data
            - Team Offense ({YEAR}): {home_team_off_2025.to_string()}
            - Active Player Stats ({YEAR}): {home_roster.to_string()}
            - Previous Season Stats ({YEAR-1}): {home_hist.to_string()}
            ---
            ## {away_team_full} (Away) Data
            - Team Offense ({YEAR}): {away_team_off_2025.to_string()}
            - Active Player Stats ({YEAR}): {away_roster.to_string()}
            - Previous Season Stats ({YEAR-1}): {away_hist.to_string()}
            ---
            Based on the data, provide the following in a clear format:
            1. **Game Prediction:** Predicted Winner and Predicted Final Score.
            2. **Score Confidence Percentage:** [Provide a confidence percentage from 1% to 100% for the predicted winner.]
            3. **Justification:** A brief justification for your prediction.
            4. **Key Player Stat Predictions:** For the starting QB, RB, and top WR for each team, provide predictions. Format each player on a new line, with each stat on its own line underneath. Include a confidence percentage. For example:
               CHI RB Khalil Herbert
               Rushing Yards: 75 - 80% confidence
            5. **Touchdown Scorers:** List 2-3 players most likely to score a **rushing or receiving** touchdown. Do not include quarterbacks for passing touchdowns. Provide a confidence percentage for each player.
            """
            
            try:
                response = model.generate_content(matchup_prompt)
                details = response.text
                winner_match = re.search(r"\*?\*?Predicted Winner:\*?\*?\s*(.*)", details)
                score_match = re.search(r"\*?\*?Predicted Final Score:\*?\*?\s*(.*)", details)
                winner = winner_match.group(1).strip() if winner_match else "See Details"
                score = score_match.group(1).strip() if score_match else "See Details"
                worksheet.update(f'D{row_num}:E{row_num}', [[winner, score]])
                worksheet.update(f'H{row_num}', [[details]])
                print(f"    -> Wrote prediction for {away_team_full} at {home_team_full}")
            except Exception as e:
                print(f"    -> Could not generate prediction: {e}")

        else:
            print(f"  -> Analyzing completed game...")
            if boxscore_link and 'boxscores' in boxscore_link:
                full_boxscore_url = "https://www.pro-football-reference.com" + boxscore_link
                box_score_data = scrape_box_score(full_boxscore_url)
                if box_score_data:
                     scores = box_score_data['final_score'].split('-')
                     away_score_actual = int(scores[0])
                     home_score_actual = int(scores[1])
                     actual_winner = away_team_full if away_score_actual > home_score_actual else home_team_full
                     
                     worksheet.update(f'F{row_num}:G{row_num}', [[actual_winner, box_score_data['final_score']]])
                     
                     player_stats_df = box_score_data.get("player_stats")
                     if player_stats_df is not None and not player_stats_df.empty:
                         stats_string = player_stats_df.to_string(index=False)
                         worksheet.update(f'I{row_num}', [[stats_string]])
                     print(f"    -> Updated actuals for {away_team_full} at {home_team_full}")
            else:
                print(f"    -> Box score link not found for this game.")
            
        time.sleep(10)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction script finished.")

if __name__ == "__main__":
    main()
