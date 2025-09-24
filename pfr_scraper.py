import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup, Comment
import os
import pickle
import re
from io import StringIO
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
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

def clean_pfr_table(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(1)
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk'].copy()
    df = df.dropna(how='all').reset_index(drop=True)
    if 'Tm' in df.columns:
        df = df[~df['Tm'].str.contains('AFC|NFC|Avg Team|League Total', na=False, case=False)]
    return df

# --- MAIN SCRIPT FOR DAILY DATA DUMP ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    headers = {'User-Agent': 'Mozilla/5.0'}

    for stat_type in [("Offense", "team_stats"), ("Defense", "opp_stats")]:
        print(f"\n--- Scraping PFR TEAM {stat_type[0].upper()} ({YEAR}) ---")
        try:
            url_suffix = "opp.htm" if stat_type[0] == "Defense" else ""
            url = f"https://www.pro-football-reference.com/years/{YEAR}/{url_suffix}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table_html = None
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                if f'id="div_{stat_type[1]}"' in comment:
                    table_html = comment
                    break
            
            if not table_html:
                table_html = soup.find('table', id=stat_type[1])

            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]
                sheet_name = "D_Overall" if stat_type[0] == "Defense" else "O_Team_Overall"
                write_to_sheet(spreadsheet, sheet_name, clean_pfr_table(df))
            else:
                 raise ValueError(f"Could not find the stats table '{stat_type[1]}'.")
        except Exception as e:
            print(f"❌ Could not process Team {stat_type[0]} Stats for {YEAR}: {e}")

    for year in [YEAR, YEAR - 1]:
        print(f"\n--- Scraping PFR PLAYER OFFENSE ({year}) ---")
        prefix = "" if year == YEAR else f"{year}_"
        try:
            passing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{year}/passing.htm")[0]
            rushing_df = pd.read_html(f"https://www.pro-football-reference.com/years/{year}/rushing.htm")[0]
            receiving_df = pd.read_html(f"https://www.pro-football-reference.com/years/{year}/receiving.htm")[0]
            write_to_sheet(spreadsheet, f"{prefix}O_Player_Passing", clean_pfr_table(passing_df))
            write_to_sheet(spreadsheet, f"{prefix}O_Player_Rushing", clean_pfr_table(rushing_df))
            write_to_sheet(spreadsheet, f"{prefix}O_Player_Receiving", clean_pfr_table(receiving_df))
        except Exception as e:
            print(f"❌ Could not process Player Offensive Stats for {year}: {e}")
            
    print(f"\n--- Scraping PFR TEAM DEFENSE ({YEAR - 1}) ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR - 1}/opp.htm"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table_html = None
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if 'div_opp_stats' in comment:
                table_html = comment
                break
        if not table_html:
            table_html = soup.find('table', id='opp_stats')
        if table_html:
            df = pd.read_html(StringIO(str(table_html)))[0]
            write_to_sheet(spreadsheet, f"{YEAR-1}_D_Overall", clean_pfr_table(df))
        else:
            raise ValueError(f"Could not find the team defense stats table for {YEAR-1}.")
    except Exception as e:
        print(f"❌ Could not process Team Defense Stats for {YEAR - 1}: {e}")

    print("\n--- Scraping FootballGuys.com Depth Charts (with Status) ---")
    try:
        url = "https://www.footballguys.com/depth-charts"
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
            depth_chart_df = pd.DataFrame(all_players)
            write_to_sheet(spreadsheet, "Depth_Charts", depth_chart_df)
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")

    print("\n✅ Scraper script finished.")
