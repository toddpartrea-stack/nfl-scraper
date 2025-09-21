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
import re

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Google Sheets Authentication ---
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

# --- Helper Function to Write to a Sheet Tab ---
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
    
# --- Advanced Data Cleaning Helper ---
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

    print("\n--- Scraping TeamRankings.com Power Rankings ---")
    try:
        url = "https://www.teamrankings.com/nfl/rankings/teams/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        all_tables = pd.read_html(url, header=0)
        rankings_df = all_tables[0]
        write_to_sheet(spreadsheet, "Power_Rankings", rankings_df)
    except Exception as e:
        print(f"❌ Could not process TeamRankings Stats: {e}")
    
    print("\n--- Scraping PFR DEFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    # --- FRANCO: NEW, MORE RESILIENT LOGIC FOR TEAM OFFENSE ---
    print("\n--- Scraping PFR TEAM OFFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        all_tables = pd.read_html(url)
        team_offense_df = None
        # Find the correct table by looking for a unique column, like 'PF' (Points For)
        for table in all_tables:
            if 'PF' in table.columns:
                team_offense_df = table
                break # Stop looking once we've found it
        
        if team_offense_df is not None:
            write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
        else:
            print("❌ Could not find the Team Offense table by looking for the 'PF' column.")
    except Exception as e: 
        print(f"❌ Could not process Team Offensive Stats: {e}")
    
    print("\n--- Scraping PFR PLAYER OFFENSE ---")
    try:
        passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm")[0]
        rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm")[0]
        receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm")[0]
        write_to_sheet(spreadsheet, "O_Player_Passing", clean_pfr_table(passing_df))
        write_to_sheet(spreadsheet, "O_Player_Rushing", clean_pfr_table(rushing_df))
        write_to_sheet(spreadsheet, "O_Player_Receiving", clean_pfr_table(receiving_df))
    except Exception as e: print(f"❌ Could not process Player Offensive Stats: {e}")

    print("\n--- Scraping CBS Sports INJURIES ---")
    try:
        url = "https://www.cbssports.com/nfl/injuries/"
        headers = {'User-Agent': 'Mozilla/5.0'}
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

    print("\n--- Scraping FootballGuys.com Depth Charts (with Status) ---")
    try:
        url = "https://www.footballguys.com/depth-charts"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        team_containers = soup.find_all('div', class_='depth-chart')
        all_players = []
        status_pattern = re.compile(r'(.+?)\s+\(([A-Z-]+)\)$')

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
                            clean_name, status = player_text, 'Healthy'
                            match = status_pattern.match(player_text)
                            if match:
                                clean_name, status = match.group(1).strip(), match.group(2).strip()
                            all_players.append({
                                'Team': team_name, 'Position': position, 'Depth': i + 1, 
                                'Player': clean_name, 'Status': status
                            })
        if all_players:
            depth_chart_df = pd.DataFrame(all_players)
            write_to_sheet(spreadsheet, "Depth_Charts", depth_chart_df)
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")

    print("\n✅ Scraper script finished.")
