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

        # 1. Get Official Schedule from API
        print(f"\n--- Fetching Official Schedule from API ({YEAR}) ---")
        try:
            games_data = get_api_data("games", {"league": "1", "season": YEAR})
            if games_data:
                schedule_list = []
                for item in games_data:
                    schedule_list.append({
                        'GameID': item['game']['id'],
                        'Week': item['league']['round'],
                        'Date': item['game']['date']['date'],
                        'Time': item['game']['date']['time'],
                        'Away Team': item['teams']['away']['name'],
                        'Home Team': item['teams']['home']['name']
                    })
                schedule_df = pd.DataFrame(schedule_list)
                schedule_df['Week'] = schedule_df['Week'].str.replace('Week ', '', regex=False).astype(int)
                write_to_sheet(spreadsheet, "Schedule", schedule_df)
        except Exception as e:
            print(f"❌ Could not process Schedule from API: {e}")

        # 2. Get Team Standings
        print(f"\n--- Fetching Team Standings from API ({YEAR}) ---")
        try:
            standings_data = get_api_data("standings", {"league": "1", "season": YEAR})
            if standings_data:
                all_teams_stats = []
                for conference in standings_data:
                    for team_info in conference:
                        all_teams_stats.append({
                            'Tm': team_info['team']['name'],
                            'W': team_info['won'], 'L': team_info['lost'], 'T': team_info['ties'],
                            'PF': team_info['points']['for'], 'PA': team_info['points']['against']
                        })
                df = pd.DataFrame(all_teams_stats)
                write_to_sheet(spreadsheet, "O_Team_Overall", df[['Tm', 'W', 'L', 'T', 'PF']].copy())
                write_to_sheet(spreadsheet, "D_Overall", df[['Tm', 'PA']].copy())
        except Exception as e:
            print(f"❌ Could not process Team Standings: {e}")

        # 3. Get Player Stats
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
                        p_info = {'Player': p['player']['name'], 'Tm': p['team']['name'], 'G': p['games']['appearences']}
                        for group in p['statistics']:
                            stats = {s['name']: s['value'] for s in group['statistics']}
                            if group['name'] == 'Passing': passing.append({**p_info, **stats})
                            elif group['name'] == 'Rushing': rushing.append({**p_info, **stats})
                            elif group['name'] == 'Receiving': receiving.append({**p_info, **stats})
                    prefix = "" if year_to_fetch == YEAR else f"{year_to_fetch}_"
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Passing", pd.DataFrame(passing))
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Rushing", pd.DataFrame(rushing))
                    write_to_sheet(spreadsheet, f"{prefix}O_Player_Receiving", pd.DataFrame(receiving))
            except Exception as e:
                print(f"❌ Could not process Player Stats for {year_to_fetch}: {e}")
        
        # 4. Scrape Depth Charts
        print("\n--- Scraping FootballGuys.com Depth Charts ---")
        try:
            url = "https://www.footballguys.com/depth-charts"
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(response.content, 'html.parser')
            team_containers = soup.find_all('div', class_='depth-chart')
            all_players = []
            for container in team_containers:
                team_name_tag = container.find('span', class_='team-header')
                if team_name_tag:
                    team_name = team_name_tag.text.strip()
                    position_items = container.find_all('li')
                    for item in position_items:
                        pos_label_tag = item.find('span', class_='pos-label')
                        if pos_label_tag:
                            position = pos_label_tag.text.replace(':', '').strip()
                            player_tags = item.find_all(['a', 'span'], class_='player')
                            for i, player_tag in enumerate(player_tags):
                                player_text = player_tag.text.strip()
                                clean_name = re.sub(r'\s+\([A-Z-]+\)$', '', player_text).strip()
                                status_match = re.search(r'\(([A-Z-]+)\)$', player_text)
                                status = status_match.group(1) if status_match else 'Healthy'
                                all_players.append({'Team': team_name, 'Position': position, 'Depth': i + 1, 'Player': clean_name, 'Status': status})
            if all_players:
                write_to_sheet(spreadsheet, "Depth_Charts", pd.DataFrame(all_players))
        except Exception as e:
            print(f"❌ Could not process Depth Charts: {e}")

        print("\n✅ Scraper script finished.")
