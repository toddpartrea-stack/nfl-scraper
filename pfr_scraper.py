import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
import os
import pickle
import re
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
FOOTBALL_API_KEY = os.getenv('FOOTBALL_API_KEY') # Get the new API key from GitHub secrets

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
        print(f"  -> Dataframe for {sheet_name} is empty, skipping write.")
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

# --- API-Powered Functions ---
def get_team_standings(api_key, year):
    print("\n--- Fetching Team Standings from API-Football ---")
    url = "https://api-football-v1.p.rapidapi.com/v3/standings"
    querystring = {"season": str(year), "league": "1"} # League ID 1 is the NFL
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()['response']
        
        if not data:
            print("  -> API returned no standings data.")
            return pd.DataFrame(), pd.DataFrame()

        standings_data = data[0]['league']['standings'][0]
        
        all_teams_stats = []
        for team_info in standings_data:
            all_teams_stats.append({
                'Tm': team_info['team']['name'],
                'W': team_info['all']['win'],
                'L': team_info['all']['lose'],
                'T': team_info['all']['draw'],
                'PF': team_info['all']['goals']['for'],
                'PA': team_info['all']['goals']['against'],
                'Net': team_info['points'], # Using 'points' for Net, can be adjusted
            })
        
        df = pd.DataFrame(all_teams_stats)
        # Create separate dataframes for offense and defense for compatibility
        offense_df = df[['Tm', 'W', 'L', 'T', 'PF']].copy()
        defense_df = df[['Tm', 'PA']].copy()
        return offense_df, defense_df
    except Exception as e:
        print(f"❌ Could not process Team Standings: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- Web Scraping Functions ---
def scrape_depth_charts():
    print("\n--- Scraping FootballGuys.com Depth Charts (with Status) ---")
    try:
        url = "https://www.footballguys.com/depth-charts"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
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
            return pd.DataFrame(all_players)
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")
    return pd.DataFrame()

# --- MAIN SCRIPT ---
if __name__ == "__main__":
    if not FOOTBALL_API_KEY:
        print("❌ ERROR: FOOTBALL_API_KEY secret not found. Please add it to your GitHub repository secrets.")
    else:
        print("Authenticating with Google Sheets...")
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
        
        # Get data from the API
        offense_df, defense_df = get_team_standings(FOOTBALL_API_KEY, YEAR)
        
        # Write API data to sheets
        if not offense_df.empty:
            write_to_sheet(spreadsheet, "O_Team_Overall", offense_df)
        if not defense_df.empty:
            write_to_sheet(spreadsheet, "D_Overall", defense_df)

        # Scrape depth charts from the web
        depth_chart_df = scrape_depth_charts()
        if not depth_chart_df.empty:
            write_to_sheet(spreadsheet, "Depth_Charts", depth_chart_df)

        print("\n✅ Scraper script finished.")
