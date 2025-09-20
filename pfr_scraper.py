# pfr_scraper.py (COMPLETE TEMPORARY VERSION FOR 2024 SCRAPE)
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
YEAR = 2024 # CHANGED TO 2024 FOR THIS ONE-TIME RUN
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

# --- HELPER FUNCTION WITH PREFIX LOGIC ---
def write_to_sheet(spreadsheet, sheet_name, dataframe):
    prefixed_sheet_name = f"2024_{sheet_name}"
    print(f"  -> Writing data to '{prefixed_sheet_name}' tab...")
    if dataframe.empty:
        print(f"  -> Data is empty for {prefixed_sheet_name}.")
        return
    try:
        worksheet = spreadsheet.worksheet(prefixed_sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=prefixed_sheet_name, rows=1, cols=len(dataframe.columns))
    
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
    # This site doesn't have historical data, so we skip it for the 2024 run
    print("  -> Skipping for historical run.")
    
    print("\n--- Scraping PFR DEFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/opp.htm"
        all_tables = pd.read_html(url)
        if len(all_tables) > 0: write_to_sheet(spreadsheet, "D_Overall", clean_pfr_table(all_tables[0]))
    except Exception as e: print(f"❌ Could not process Defensive Stats: {e}")

    print("\n--- Scraping PFR TEAM OFFENSE ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{YEAR}/"
        team_offense_df = pd.read_html(url, attrs={'id': 'team_stats'})[0]
        write_to_sheet(spreadsheet, "O_Team_Overall", clean_pfr_table(team_offense_df))
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

    # We don't need historical injuries or depth charts, so we skip them
    print("\n--- Skipping CBS Sports INJURIES (historical) ---")
    print("\n--- Skipping FootballGuys.com Depth Charts (historical) ---")

    print("\n✅ Historical scraper script finished.")
