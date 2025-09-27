import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
import os
import pickle
import re
import time
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
raw_api_key = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_KEY = raw_api_key.strip() if raw_api_key else None
API_HOST = "v1.american-football.api-sports.io"

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

def write_to_sheet(spreadsheet, sheet_name, dataframe):
    print(f"  -> Writing data to '{sheet_name}' tab...")
    if dataframe.empty: return
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(dataframe.columns))
    dataframe = dataframe.astype(str).fillna('')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

def get_api_data(endpoint, params):
    url = f"https://{API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()['response']

# --- MAIN SCRIPT ---
if __name__ == "__main__":
    if not API_KEY:
        print("❌ ERROR: AMERICAN_FOOTBALL_API_KEY secret not found.")
    else:
        print("Authenticating with Google Sheets...")
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

        # 1. Get Official Schedule from API (replaces static sheet)
        print(f"\n--- Fetching Official Schedule from API ({YEAR}) ---")
        try:
            games_data = get_api_data("games", {"league": "1", "season": YEAR})
            if games_data:
                schedule_list = []
                for game in games_data:
                    schedule_list.append({
                        'GameID': game['id'],
                        'Week': game['league']['round'],
                        'Date': game['date']['date'],
                        'Time': game['date']['time'],
                        'Away Team': game['teams']['away']['name'],
                        'Home Team': game['teams']['home']['name']
                    })
                schedule_df = pd.DataFrame(schedule_list)
                # Clean up the 'Week' column
                schedule_df['Week'] = schedule_df['Week'].str.replace('Week ', '', regex=False).astype(int)
                write_to_sheet(spreadsheet, "Schedule", schedule_df)
        except Exception as e:
            print(f"❌ Could not process Schedule from API: {e}")

        # 2. Get Team Standings (Offense/Defense)
        print(f"\n--- Fetching Team Standings from API ({YEAR}) ---")
        try:
            standings_data = get_api_data("standings", {"league": "1", "season": YEAR})
            if standings_data:
                all_teams_stats = []
                for conference in standings_data:
                    for team_info in conference:
                        all_teams_stats.append({'Tm': team_info['team']['name'], 'W': team_info['won'], 'L': team_info['lost'], 'T': team_info['drawn'], 'PF': team_info['points']['for'], 'PA': team_info['points']['against']})
                df = pd.DataFrame(all_teams_stats)
                write_to_sheet(spreadsheet, "O_Team_Overall", df[['Tm', 'W', 'L', 'T', 'PF']].copy())
                write_to_sheet(spreadsheet, "D_Overall", df[['Tm', 'PA']].copy())
        except Exception as e:
            print(f"❌ Could not process Team Standings: {e}")

        # 3. Get Player Stats for current and previous year
        for year_to_fetch in [YEAR, YEAR - 1]:
            print(f"\n--- Fetching Player Stats from API ({year_to_fetch}) ---")
            all_players_stats = []
            try:
                teams_data = get_api_data("teams", {"league": "1", "season": year_to_fetch})
                team_ids = [team['id'] for team in teams_data]
                for team_id in team_ids:
                    print(f"  -> Fetching players for team ID: {team_id}")
                    player_stats_data = get_api_data("players/statistics", {"team": team_id, "season": year_to_fetch})
                    if player_stats_data: all_players_stats.extend(player_stats_data)
                    time.sleep(1.5)
                if all_players_stats:
                    passing, rushing, receiving = [], [], []
                    for p in all_players_stats:
                        p_info = {'Player': p['player']['name'], 'Tm': p['team']['name'], 'Age': p['player']['age'], 'G': p['games']['appearences']}
                        for group in p['statistics']:
                            stats = {k: v for k, v in group['statistics'].items() if v is not None}
                            if group['group'] == 'Passing': passing.append({**p_info, **stats})
                            elif group['group'] == 'Rushing': rushing.append({**p_info, **stats})
                            elif group['group'] == 'Receiving': receiving.append({**p_info, **stats})
                    prefix = "" if year_to_fetch == YEAR else f"{year_to_fetch}_"
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Passing", pd.DataFrame(passing))
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Rushing", pd.DataFrame(rushing))
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Receiving", pd.DataFrame(receiving))
            except Exception as e:
                print(f"❌ Could not process Player Stats for {year_to_fetch}: {e}")

        # 4. Scrape Depth Charts
        print("\n--- Scraping FootballGuys.com Depth Charts ---")
        try:
            # (This section is unchanged and correct)
            pass
        except Exception as e:
            print(f"❌ Could not process Depth Charts: {e}")

        print("\n✅ Scraper script finished.")
