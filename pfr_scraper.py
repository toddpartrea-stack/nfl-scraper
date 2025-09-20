import requests
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import io
from datetime import datetime
from bs4 import BeautifulSoup

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
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ An error occurred opening the sheet: {e}")
        exit()

    # --- (Your working TeamRankings scraping logic) ---
    print("\n--- Scraping TeamRankings.com Power Rankings ---")
    try:
        url = "https://www.teamrankings.com/nfl/rankings/teams/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        }
        all_tables = pd.read_html(url, header=0)
        rankings_df = all_tables[0]
        write_to_sheet(spreadsheet, "Power_Rankings", rankings_df)
    except Exception as e:
        print(f"❌ Could not process TeamRankings Stats: {e}")
    
    # --- DEFENSE (Your working version) ---
    print("\n--- Scraping DEFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
        if len(all_tables) > 2: write_to_sheet(spreadsheet, "D_Passing", clean_pfr_table(all_tables[2]))
        if len(all_tables) > 3: write_to_sheet(spreadsheet, "D_Rushing", clean_pfr_table(all_tables[3]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    # --- OFFENSE (TEAM) (Your working version) ---
    print("\n--- Scraping TEAM OFFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        try:
            team_offense_df = pd.read_html(url, match="Team Offense")[0]
            write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
        except ValueError:
            print("  -> Team Offense table not found (likely not posted for the new season yet).")
    except Exception as e: print(f"❌ Could not process Team Offensive Stats: {e}")
    
    # --- OFFENSE (PLAYER) (Your working version) ---
    print("\n--- Scraping PLAYER OFFENSE ---")
    try:
        passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/passing.htm")[0]
        rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/rushing.htm")[0]
        receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/receiving.htm")[0]
        write_to_sheet(spreadsheet, "O_Player_Passing", clean_pfr_table(passing_df))
        write_to_sheet(spreadsheet, "O_Player_Rushing", clean_pfr_table(rushing_df))
        write_to_sheet(spreadsheet, "O_Player_Receiving", clean_pfr_table(receiving_df))
    except Exception as e: print(f"❌ Could not process Player Offensive Stats: {e}")

    # --- NEW: Scrape CBS Sports Injury Report ---
    print("\n--- Scraping CBS Sports INJURIES ---")
    try:
        url = "https://www.cbssports.com/nfl/injuries/"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh...)'} # Abridged for clarity
        response = requests.get(url, headers=headers)
        response.raise_for_status()
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
                        table_headers = [th.text for th in table.find('thead').find_all('th')]
                        table_rows = []
                        for tr in table.find('tbody').find_all('tr'):
                            cells = [td.text.strip() for td in tr.find_all('td')]
                            # Logic from your working ir_test.py to find the correct player name
                            long_name_span = tr.find('span', class_='CellPlayerName--long')
                            if long_name_span:
                                cells[0] = long_name_span.text.strip()
                            table_rows.append(cells)

                        df = pd.DataFrame(table_rows, columns=table_headers)
                        df['Team'] = team_name
                        all_injuries.append(df)

        if all_injuries:
            final_df = pd.concat(all_injuries, ignore_index=True)
            write_to_sheet(spreadsheet, "Injuries", final_df)
        else:
            print("  -> Could not find any injury tables on the page.")
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

    # --- SCHEDULE (Your working version) ---
    print("\n--- Scraping SCHEDULE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/games.htm"
        schedule_df = pd.read_html(url)[0]
        write_to_sheet(spreadsheet, "Schedule", clean_pfr_table(schedule_df))
    except Exception as e: print(f"❌ Could not process Schedule: {e}")

    print("\n✅ Project script finished.")
