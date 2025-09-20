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

    # --- (Your PFR scraping logic for Defense, Offense, Players, etc.) ---
    # ...
    
    # --- NEW: Scrape CBS Sports Injury Report ---
    print("\n--- Scraping CBS Sports INJURIES ---")
    try:
        url = "https://www.cbssports.com/nfl/injuries/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        }
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

    # --- SCHEDULE (Your working version) ---
    print("\n--- Scraping SCHEDULE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/games.htm"
        schedule_df = pd.read_html(url)[0]
        write_to_sheet(spreadsheet, "Schedule", clean_pfr_table(schedule_df))
    except Exception as e: 
        print(f"❌ Could not process Schedule: {e}")

    print("\n✅ Project script finished.")
