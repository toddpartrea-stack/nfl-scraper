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
import re
from datetime import datetime

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
    
    rk_col = next((col for col in df.columns if 'Rk' in col), None)
    if rk_col:
        df = df[df[rk_col] != 'Rk'].copy()
        
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def scrape_box_score(box_score_url):
    print(f"    -> Scraping box score: {box_score_url}")
    if not box_score_url or not isinstance(box_score_url, str) or 'boxscores' not in box_score_url:
        return None
    try:
        response = requests.get(box_score_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text.replace('', ''), 'html.parser')

        scorebox = soup.find('div', class_='scorebox')
        away_score = scorebox.find('div', class_='scores away').text.strip()
        home_score = scorebox.find('div', class_='scores home').text.strip()
        final_score = f"{away_score}-{home_score}"

        player_offense_table = soup.find('table', id='player_offense')
        if player_offense_table is None:
             return {"final_score": final_score, "player_stats": pd.DataFrame()}
        
        stats_df = pd.read_html(io.StringIO(str(player_offense_table)))[0]
        stats_df.columns = ['_'.join(col).strip() for col in stats_df.columns]
        stats_df = stats_df[stats_df['Player_Player'] != 'Player'].copy()
        
        key_stats = {
            'Player_Player': 'Player', 'Passing_Cmp': 'PassCmp', 'Passing_Att': 'PassAtt',
            'Passing_Yds': 'PassYds', 'Passing_TD': 'PassTD', 'Rushing_Att': 'RushAtt',
            'Rushing_Yds': 'RushYds', 'Rushing_TD': 'RushTD', 'Receiving_Tgt': 'RecTgt',
            'Receiving_Rec': 'Rec', 'Receiving_Yds': 'RecYds', 'Receiving_TD': 'RecTD'
        }
        cols_to_keep = [col for col in key_stats.keys() if col in stats_df.columns]
        final_stats_df = stats_df[cols_to_keep].rename(columns=key_stats)
        
        return {"final_score": final_score, "player_stats": final_stats_df}
    except Exception as e:
        print(f"    -> An error occurred scraping the box score: {e}")
        return None

def scrape_schedule(year):
    print("\n--- Scraping PFR SCHEDULE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{year}/games.htm"
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table', id='games')
        if not table:
            print("❌ Could not find schedule table.")
            return pd.DataFrame()

        rows = []
        for row in table.find('tbody').find_all('tr'):
            if row.find('th', class_='thead'): continue
            
            week = row.find('th', {'data-stat': 'week_num'}).text
            day = row.find('td', {'data-stat': 'game_day_of_week'}).text
            date = row.find('td', {'data-stat': 'game_date'}).text
            time = row.find('td', {'data-stat': 'gametime'}).text
            visitor = row.find('td', {'data-stat': 'visitor_team'}).text
            home = row.find('td', {'data-stat': 'home_team'}).text
            
            boxscore_cell = row.find('td', {'data-stat': 'boxscore_word'})
            boxscore_link = boxscore_cell.find('a')['href'] if boxscore_cell and boxscore_cell.find('a') else ''
            
            rows.append([week, day, date, time, visitor, home, boxscore_link])

        headers = ["Week", "Day", "Date", "Time", "Visitor", "Home", "Boxscore"]
        df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception as e:
        print(f"❌ Could not process Schedule: {e}")
        return pd.DataFrame()

# --- MAIN SCRIPT FOR DAILY DATA DUMP ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    schedule_df = scrape_schedule(YEAR)
    if not schedule_df.empty:
        write_to_sheet(spreadsheet, "Schedule", schedule_df)

    print("\n--- Scraping PFR TEAM OFFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        all_tables = pd.read_html(url)
        team_offense_df = None
        for table in all_tables:
            if 'PF' in table.columns:
                team_offense_df = table
                break
        if team_offense_df is not None:
            write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
        else:
            print("❌ Could not find the Team Offense table.")
    except Exception as e: 
        print(f"❌ Could not process Team Offensive Stats: {e}")

    print("\n✅ Scraper script finished.")
