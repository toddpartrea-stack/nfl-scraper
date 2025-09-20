import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import io
from datetime import datetime

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Google Sheets Authentication (Your working version) ---
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

# --- Helper Function to Write to a Sheet Tab (Your working version) ---
def write_to_sheet(spreadsheet, sheet_name, dataframe):
    print(f"  -> Writing data to '{sheet_name}' tab...")
    if dataframe.empty:
        print(f"  -> Data is empty for {sheet_name}.")
        return

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(dataframe.columns))

    dataframe = dataframe.astype(str).fillna('')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

# --- Advanced Data Cleaning Helper (Your working version) ---
def clean_pfr_table(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk'].copy()
    df = df.dropna(how='all').reset_index(drop=True)
    return df

# --- Main Script ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # --- Your working PFR and TeamRankings scraping logic ---
    print("\n--- Scraping TeamRankings.com Power Rankings ---")
    try:
        url = "https://www.teamrankings.com/nfl/rankings/teams/"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh...)'} # Abridged for clarity
        all_tables = pd.read_html(url, header=0)
        rankings_df = all_tables[0]
        write_to_sheet(spreadsheet, "Power_Rankings", rankings_df)
    except Exception as e:
        print(f"❌ Could not process TeamRankings Stats: {e}")

    print("\n--- Scraping DEFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
        if len(all_tables) > 2: write_to_sheet(spreadsheet, "D_Passing", clean_pfr_table(all_tables[2]))
        if len(all_tables) > 3: write_to_sheet(spreadsheet, "D_Rushing", clean_pfr_table(all_tables[3]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    # ... (Your other PFR scraping logic for Team Offense, Player Offense, Schedule) ...
    
    # --- NEW: Scrape CBS Sports Injury Report ---
    print("\n--- Scraping CBS Sports INJURIES ---")
    try:
        url = "https://www.cbssports.com/nfl/injuries/"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh...)'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        team_headers = soup.find_all('h4', class_='TableBase-title')
        all_injuries = []
        for header in team_headers:
            team_name_tag = header.find('span', class_='TeamName')
            if team_name_tag:
                team_name = team_name_tag.text.strip()
                table_container = header.find_parent('div', class_='TableBase')
                if table_container:
                    table = table_container.find('table')
                    if table:
                        df = pd.read_html(io.StringIO(str(table)))[0]
                        df['Team'] = team_name
                        df['Player'] = df['Player'].apply(lambda x: ' '.join(x.split()[1:]) if isinstance(x, str) else x)
                        all_injuries.append(df)
        if all_injuries:
            final_df = pd.concat(all_injuries, ignore_index=True)
            write_to_sheet(spreadsheet, "Injuries", final_df)
    except Exception as e: 
        print(f"❌ Could not process Injury Reports: {e}")

    # --- NEW: Scrape FootballGuys.com Depth Charts ---
    print("\n--- Scraping FootballGuys.com Depth Charts ---")
    try:
        url = "https://www.footballguys.com/depth-charts"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh...)'}
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
                            player_name = player_tag.text.strip()
                            all_players.append({'Team': team_name, 'Position': position, 'Depth': i + 1, 'Player': player_name})
        if all_players:
            depth_chart_df = pd.DataFrame(all_players)
            write_to_sheet(spreadsheet, "Depth_Charts", depth_chart_df)
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")

    print("\n✅ Scraper script finished.")
