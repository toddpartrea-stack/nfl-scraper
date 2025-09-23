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
        print(f"  -> Data is empty for {sheet_name}.")
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
        df = df[~df['Tm'].str.contains('AFC|NFC|Avg Team|League Total', na=False)]
    return df

# --- START: FINAL, ROBUST scrape_box_score FUNCTION ---
def scrape_box_score(box_score_url):
    print(f"    -> Scraping box score: {box_score_url}")
    if not box_score_url or 'boxscores' not in box_score_url:
        return None
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(box_score_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the scorebox and parse the final score
        scorebox = soup.find('div', class_='scorebox')
        if not scorebox:
            print("    -> FAILED: Could not find scorebox div.")
            return None
        scores = scorebox.find_all('div', class_='score')
        if len(scores) < 2:
            print("    -> FAILED: Could not find both team scores.")
            return None
        away_score, home_score = scores[0].text.strip(), scores[1].text.strip()
        final_score = f"{away_score}-{home_score}"

        # Find the hidden player stats table within the HTML comments
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        table_html = None
        for comment in comments:
            if 'id="player_offense"' in comment:
                table_html = comment
                break
        
        if not table_html:
            print("    -> FAILED: Could not find hidden player_offense table in comments.")
            return None
            
        stats_df = pd.read_html(StringIO(table_html))[0]
        
        stats_df.columns = stats_df.columns.droplevel(0) # Drop the top-level header
        stats_df = stats_df[stats_df['Player'] != 'Player'].copy() # Remove intermittent header rows

        key_stats = {
            'Player': 'Player', 'Cmp': 'PassCmp', 'Att': 'PassAtt', 'Yds': 'PassYds', 
            'TD': 'PassTD', 'Att_2': 'RushAtt', 'Yds_2': 'RushYds', 'TD_2': 'RushTD', 
            'Tgt': 'RecTgt', 'Rec': 'Rec', 'Yds_3': 'RecYds', 'TD_3': 'RecTD'
        }
        # Rename duplicate columns like 'Yds' and 'TD' to be unique before filtering
        cols = pd.Series(stats_df.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + f'_{i}' if i != 0 else dup for i in range(sum(cols == dup))]
        stats_df.columns = cols
        
        cols_to_keep = [col for col in key_stats.keys() if col in stats_df.columns]
        final_stats_df = stats_df[cols_to_keep].rename(columns=key_stats)

        return {"final_score": final_score, "player_stats": final_stats_df}
    except Exception as e:
        print(f"    -> An error occurred scraping the box score: {e}")
        return None
# --- END: FINAL, ROBUST scrape_box_score FUNCTION ---

# --- MAIN SCRIPT FOR DAILY DATA DUMP ---
if __name__ == "__main__":
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    
    headers = {'User-Agent': 'Mozilla/5.0'}

    for stat_type in [("Offense", ""), ("Defense", "opp")]:
        print(f"\n--- Scraping PFR TEAM {stat_type[0].upper()} ---")
        try:
            page_suffix = stat_type[1]
            url = f"https://www.pro-football-reference.com/years/{YEAR}/"
            if page_suffix:
                url += f"{page_suffix}.htm"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table_html = None
            
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                if 'id="team_stats"' in comment or 'id="opp_stats"' in comment:
                    table_html = comment
                    break
            
            if not table_html:
                table_html = soup.find('table', id='team_stats') or soup.find('table', id='opp_stats')

            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]
                sheet_name = "O_Team_Overall" if stat_type[0] == "Offense" else "D_Overall"
                write_to_sheet(spreadsheet, sheet_name, clean_pfr_table(df))
            else:
                raise ValueError(f"Could not find the main team {stat_type[0].lower()} stats table (hidden or visible).")
        
        except Exception as e:
            print(f"❌ Could not process Team {stat_type[0]} Stats: {e}")

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
