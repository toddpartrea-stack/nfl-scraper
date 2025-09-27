import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
import os
import pickle
import re
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
FOOTBALL_API_KEY = os.getenv('FOOTBALL_API_KEY')

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
    if dataframe.empty:
        return
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(dataframe.columns))
    dataframe = dataframe.astype(str).fillna('')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

def clean_pfr_table(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(1)
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk'].copy()
    df = df.dropna(how='all').reset_index(drop=True)
    if 'Tm' in df.columns:
        df = df[~df['Tm'].str.contains('AFC|NFC|Avg Team|League Total', na=False, case=False)]
    return df

# --- MAIN SCRIPT ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    
    # --- API CALL FOR TEAM STATS ---
    print("\n--- Fetching Team Standings from API-Football ---")
    team_stats_url = "https://api-football-v1.p.rapidapi.com/v3/standings"
    querystring = {"season": str(YEAR - 1), "league": "1"}
    headers = {"X-RapidAPI-Key": FOOTBALL_API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        response = requests.get(team_stats_url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()['response']
        if data:
            standings_data = data[0]['league']['standings'][0]
            all_teams_stats = [{'Tm': team_info['team']['name'], 'W': team_info['all']['win'], 'L': team_info['all']['lose'], 'T': team_info['all']['draw'], 'PF': team_info['all']['goals']['for'], 'PA': team_info['all']['goals']['against']} for team_info in standings_data]
            df = pd.DataFrame(all_teams_stats)
            write_to_sheet(spreadsheet, "O_Team_Overall", df[['Tm', 'W', 'L', 'T', 'PF']].copy())
            write_to_sheet(spreadsheet, "D_Overall", df[['Tm', 'PA']].copy())
    except Exception as e:
        print(f"❌ Could not process Team Standings from API: {e}")

    # --- SCRAPING FOR PLAYER STATS ---
    print(f"\n--- Scraping PFR PLAYER OFFENSE ({YEAR}) ---")
    try:
        passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm")[0]
        rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm")[0]
        receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm")[0]
        write_to_sheet(spreadsheet, "O_Player_Passing", clean_pfr_table(passing_df))
        write_to_sheet(spreadsheet, "O_Player_Rushing", clean_pfr_table(rushing_df))
        write_to_sheet(spreadsheet, "O_Player_Receiving", clean_pfr_table(receiving_df))
    except Exception as e:
        print(f"❌ Could not process Player Offensive Stats for {YEAR}: {e}")

    # --- SCRAPING FOR DEPTH CHARTS ---
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
            depth_chart_df = pd.DataFrame(all_players)
            write_to_sheet(spreadsheet, "Depth_Charts", depth_chart_df)
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")

    print("\n✅ Scraper script finished.")
