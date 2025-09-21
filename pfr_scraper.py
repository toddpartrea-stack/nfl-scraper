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
from requests.exceptions import RequestException

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- AUTHENTICATION & HELPERS (UNCHANGED) ---
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
    
def clean_pfr_table(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk'].copy()
    df = df.dropna(how='all').reset_index(drop=True)
    return df

# --- FRANCO: NEW BOX SCORE SCRAPING FUNCTION ---
def scrape_box_score(box_score_url):
    """
    Scrapes a single Pro-Football-Reference box score page for the final score and player stats.
    """
    print(f"    -> Scraping box score: {box_score_url}")
    if not box_score_url or not isinstance(box_score_url, str) or 'boxscores' not in box_score_url:
        return None # Invalid URL

    try:
        response = requests.get(box_score_url)
        response.raise_for_status()
        
        # PFR wraps some tables in comments, so we need to find and uncomment them
        soup = BeautifulSoup(response.text.replace('', ''), 'html.parser')

        # Find final score
        scorebox = soup.find('div', class_='scorebox')
        away_score = scorebox.find('div', class_='scores away').text.strip()
        home_score = scorebox.find('div', class_='scores home').text.strip()
        final_score = f"{away_score}-{home_score}"

        # Find player stats table
        player_offense_table = soup.find('table', id='player_offense')
        if player_offense_table is None:
             print("    -> Could not find player offense table.")
             return None
        
        stats_df = pd.read_html(io.StringIO(str(player_offense_table)))[0]
        stats_df.columns = ['_'.join(col) for col in stats_df.columns] # Clean multi-level headers
        
        # Filter out header rows that are repeated in the table body
        stats_df = stats_df[stats_df['Player_Player'] != 'Player']
        
        return {
            "final_score": final_score,
            "player_stats": stats_df
        }
    except RequestException as e:
        print(f"    -> Error fetching box score URL: {e}")
        return None
    except Exception as e:
        print(f"    -> An error occurred scraping the box score: {e}")
        return None

# --- MAIN SCRIPT FOR DAILY DATA DUMP ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    print("\n--- Scraping PFR Main Pages ---")
    try:
        # Schedule (needed for box score URLs)
        schedule_df = pd.read_html(f"https://www.pro-football-reference.com/years/{YEAR}/games.htm")[0]
        write_to_sheet(spreadsheet, "Schedule", schedule_df)

        # Other stats...
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        all_tables = pd.read_html(url)
        team_offense_df = None
        for table in all_tables:
            if 'PF' in table.columns:
                team_offense_df = table
                break
        if team_offense_df is not None:
            write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
    except Exception as e: 
        print(f"❌ Could not process PFR Team Stats: {e}")
    
    # ... (All other scraping sections from the last working scraper) ...
    print("\n--- Scraping FootballGuys.com Depth Charts (with Status) ---")
    try:
        # ... (This whole section is unchanged) ...
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
