import os
import json
import re
import time
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
CURRENT_YEAR = 2025
PREVIOUS_YEAR = CURRENT_YEAR - 1
API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_HOST = "v1.american-football.api-sports.io"

# --- AUTHENTICATION & HELPERS ---
def get_gspread_client():
    # FINAL FIX: Bypassing the broken .auth.default() and using a direct, explicit method
    credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_path:
        raise ValueError("Could not find Google credentials path. The auth step in the workflow may have failed.")
    return gspread.service_account(filename=credential_path)

def write_to_sheet(spreadsheet, sheet_name, dataframe):
    print(f"  -> Writing data to '{sheet_name}' tab...")
    if dataframe.empty:
        print("  -> DataFrame is empty, skipping write.")
        return
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        time.sleep(1)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(dataframe.columns))
    
    dataframe = dataframe.astype(str).fillna('')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

def get_api_data(endpoint, params):
    url = f"https://{API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get('response', [])
    except requests.exceptions.RequestException as e:
        print(f"  -> API request failed for endpoint '{endpoint}': {e}")
        return []

def calculate_nfl_week(df):
    print("  -> Calculating week numbers from game dates...")
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce')
    df.dropna(subset=['game_date'], inplace=True)
    
    regular_season_games = df[df['game_date'].dt.month >= 9]
    if regular_season_games.empty:
        print("  -> Warning: No games found in or after September. Week calculation may be incorrect.")
        season_start_date = df['game_date'].min()
    else:
        season_start_date = regular_season_games['game_date'].min()

    start_of_week1 = season_start_date - pd.to_timedelta(season_start_date.weekday() - 3, unit='d')
    
    def get_week(date):
        if date < start_of_week1:
            return 0
        return ((date - start_of_week1).days // 7) + 1

    df['Week'] = df['game_date'].apply(get_week)
    df.drop(columns=['game_date'], inplace=True)
    return df

if __name__ == "__main__":
    if not API_KEY:
        print("❌ ERROR: AMERICAN_FOOTBALL_API_KEY secret not found.")
        exit()

    print("Authenticating to Google Sheets...")
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not connect to Google Sheets. Check Service Account setup. Error: {e}")
        exit()

    print(f"\n--- Fetching Official Schedule from API ({CURRENT_YEAR}) ---")
    try:
        games_data = get_api_data("games", {"league": "1", "season": str(CURRENT_YEAR)})
        if games_data:
            schedule_list = [{'GameID': i.get('game', {}).get('id'),'Week': 'N/A','Date': i.get('game', {}).get('date', {}).get('date'),'Time': i.get('game', {}).get('date', {}).get('time'),'Away Team': i.get('teams', {}).get('away', {}).get('name'),'Home Team': i.get('teams', {}).get('home', {}).get('name')} for i in games_data]
            schedule_df = pd.DataFrame(schedule_list)
            schedule_df = calculate_nfl_week(schedule_df)
            schedule_df = schedule_df[schedule_df['Week'] > 0].copy()
            write_to_sheet(spreadsheet, "Schedule", schedule_df)
    except Exception as e:
        print(f"❌ Could not process Schedule from API: {e}")
    print(f"\n--- Fetching Team Standings from API ({CURRENT_YEAR}) ---")
    try:
        standings_data = get_api_data("standings", {"league": "1", "season": str(CURRENT_YEAR)})
        if standings_data:
            all_teams_stats = [{'Tm': t.get('team',{}).get('name'),'W':t.get('won'),'L':t.get('lost'),'T':t.get('ties'),'PF':t.get('points',{}).get('for'),'PA':t.get('points',{}).get('against')} for t in standings_data]
            df = pd.DataFrame(all_teams_stats)
            write_to_sheet(spreadsheet, "O_Team_Overall", df[['Tm', 'W', 'L', 'T', 'PF']].copy())
            write_to_sheet(spreadsheet, "D_Overall", df[['Tm', 'PA']].copy())
    except Exception as e:
        print(f"❌ Could not process Team Standings: {e}")
    for year_to_fetch in [CURRENT_YEAR, PREVIOUS_YEAR]:
        print(f"\n--- Fetching Player Stats from API ({year_to_fetch}) ---")
        all_players_stats = []
        try:
            teams_data = get_api_data("teams", {"league": "1", "season": year_to_fetch})
            team_ids = [team['id'] for team in teams_data if team]
            for team_id in team_ids:
                print(f"  -> Fetching players for team ID: {team_id}")
                player_stats_data = get_api_data("players/statistics", {"team": team_id, "season": year_to_fetch})
                if player_stats_data: all_players_stats.extend(player_stats_data)
                time.sleep(1.5)
            if all_players_stats:
                passing, rushing, receiving = [], [], []
                for p_data in all_players_stats:
                    player_info = p_data.get('player', {})
                    if not p_data.get('teams'): continue
                    team_level_data = p_data['teams'][0]
                    team_info = team_level_data.get('team', {})
                    base_stats = {'Player':player_info.get('name'),'Tm':team_info.get('name'),'G':0}
                    for group in team_level_data.get('groups', []):
                        stats = {s['name']: s['value'] for s in group.get('statistics', [])}
                        if group.get('name') == 'Passing': passing.append({**base_stats, **stats})
                        elif group.get('name') == 'Rushing': rushing.append({**base_stats, **stats})
                        elif group.get('name') == 'Receiving': receiving.append({**base_stats, **stats})
                prefix = "" if year_to_fetch == CURRENT_YEAR else f"{year_to_fetch}_"
                write_to_sheet(spreadsheet, f"{prefix}O_Player_Passing", pd.DataFrame(passing))
                write_to_sheet(spreadsheet, f"{prefix}O_Player_Rushing", pd.DataFrame(rushing))
                write_to_sheet(spreadsheet, f"{prefix}O_Player_Receiving", pd.DataFrame(receiving))
        except Exception as e:
            print(f"❌ Could not process Player Stats for {year_to_fetch}: {e}")
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
