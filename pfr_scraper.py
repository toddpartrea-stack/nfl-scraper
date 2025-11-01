import os
import json
import re
import time
import pandas as pd
import gspread
import requests 
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
CURRENT_YEAR = 2025
PREVIOUS_YEAR = CURRENT_YEAR - 1
API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
API_HOST = "v1.american-football.api-sports.io"

# --- AUTHENTICATION & HELPERS ---
def get_gspread_client():
    credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_path:
        raise ValueError("Could not find Google credentials path. The auth step in the workflow may have failed.")
    return gspread.service_account(filename=credential_path)

def write_to_sheet(spreadsheet, sheet_name, dataframe):
    print(f"  -> Writing data to '{sheet_name}' tab...")
    if dataframe.empty:
        print("  -> DataFrame is empty, skipping write.")
        return
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        time.sleep(1)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(dataframe) + 100, cols=len(dataframe.columns))
    
    dataframe = dataframe.astype(str).fillna('0')
    data_to_upload = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    
    worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    print(f"  -> Successfully wrote {len(dataframe)} rows.")

def get_api_data(endpoint, params):
    url = f"https://{API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get('response', [])
    except requests.exceptions.RequestException as e:
        print(f"  -> API request failed for endpoint '{endpoint}': {e}")
        return []

def calculate_nfl_week(df):
    print("  -> Calculating week numbers from game dates...")
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce')
    df.dropna(subset=['game_date'], inplace=True)
    
    regular_season_games = df[df['game_date'].dt.month >= 9]
    if regular_season_games.empty:
        season_start_date = df['game_date'].min()
    else:
        season_start_date = regular_season_games['game_date'].min()

    start_of_week1 = season_start_date - pd.to_timedelta(season_start_date.weekday() - 3, unit='d')
    
    def get_week(date):
        if date < start_of_week1: return 0
        return ((date - start_of_week1).days // 7) + 1

    df['Week'] = df['game_date'].apply(get_week)
    df.drop(columns=['game_date'], inplace=True)
    return df

if __name__ == "__main__":
    if not API_KEY:
        print("❌ ERROR: AMERICAN_FOOTBALL_API_KEY secret not found.")
        exit()

    print("Authenticating to Google Sheets...")
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not connect to Google Sheets. Error: {e}")
        exit()

    print(f"\n--- Fetching Official Schedule from API ({CURRENT_YEAR}) ---")
    schedule_df = pd.DataFrame() # Initialize empty dataframe
    try:
        games_data = get_api_data("games", {"league": "1", "season": str(CURRENT_YEAR)})
        if games_data:
            schedule_list = []
            for i in games_data:
                game_info = i.get('game', {})
                date_info = game_info.get('date', {})
                team_info = i.get('teams', {})
                venue_info = game_info.get('venue', {})
                
                schedule_list.append({
                    'GameID': game_info.get('id'),
                    'Week': 'N/A',
                    'Date': date_info.get('date'),
                    'Time': date_info.get('time'),
                    'Away Team': team_info.get('away', {}).get('name'),
                    'Home Team': team_info.get('home', {}).get('name'),
                    'Venue_City': venue_info.get('city'),
                    'Venue_Country': venue_info.get('country')
                })

            schedule_df = pd.DataFrame(schedule_list)
            schedule_df = calculate_nfl_week(schedule_df)
            schedule_df = schedule_df[schedule_df['Week'] > 0].copy()
            cols = ['GameID', 'Week', 'Date', 'Time', 'Away Team', 'Home Team', 'Venue_City', 'Venue_Country']
            write_to_sheet(spreadsheet, "Schedule", schedule_df[cols])
    except Exception as e:
        print(f"❌ Could not process Schedule from API: {e}")
        
    print(f"\n--- Fetching Team Standings from API ({CURRENT_YEAR}) ---")
    try:
        standings_data = get_api_data("standings", {"league": "1", "season": str(CURRENT_YEAR)})
        if standings_data:
            all_teams_stats = [{'Tm': t.get('team',{}).get('name'),'W':t.get('won'),'L':t.get('lost'),'T':t.get('ties'),'PF':t.get('points',{}).get('for'),'PA':t.get('points',{}).get('against')} for t in standings_data]
            df = pd.DataFrame(all_teams_stats)
            write_to_sheet(spreadsheet, "O_Team_Overall", df[['Tm', 'W', 'L', 'T', 'PF']].copy())
            write_to_sheet(spreadsheet, "D_Overall", df[['Tm', 'PA']].copy())
    except Exception as e:
        print(f"❌ Could not process Team Standings: {e}")

    for year_to_fetch in [CURRENT_YEAR, PREVIOUS_YEAR]:
        print(f"\n--- Fetching Player Stats from API ({year_to_fetch}) ---")
        
        all_players_stats = [] 
        
        try:
            teams_data = get_api_data("teams", {"league": "1", "season": year_to_fetch})
            team_ids = [team['id'] for team in teams_data if team]
            for team_id in team_ids:
                print(f"  -> Fetching players for team ID: {team_id}")
                player_stats_data = get_api_data("players/statistics", {"team": team_id, "season": year_to_fetch})
                if player_stats_data: all_players_stats.extend(player_stats_data)
                time.sleep(1.5)

            if all_players_stats:
                passing, rushing, receiving = [], [], []
                
                for p_data in all_players_stats:
                    player_info = p_data.get('player', {})
                    if not p_data.get('teams'): continue
                    team_level_data = p_data['teams'][0]
                    team_info = team_level_data.get('team', {})
                    base_stats = {'Player':player_info.get('name'),'Tm':team_info.get('name')}
                    for group in team_level_data.get('groups', []):
                        stats = {s['name']: s['value'] for s in group.get('statistics', [])}
                        if group.get('name') == 'Passing': passing.append({**base_stats, **stats})
                        elif group.get('name') == 'Rushing': rushing.append({**base_stats, **stats})
                        elif group.get('name') == 'Receiving': receiving.append({**base_stats, **stats})
                
                prefix = "" if year_to_fetch == CURRENT_YEAR else f"{year_to_fetch}_"
                
                df_passing = pd.DataFrame(passing)
                df_rushing = pd.DataFrame(rushing)
                df_receiving = pd.DataFrame(receiving)

                if not df_passing.empty: df_passing.drop_duplicates(subset=['Player'], keep='last', inplace=True)
                if not df_rushing.empty: df_rushing.drop_duplicates(subset=['Player'], keep='last', inplace=True)
                if not df_receiving.empty: df_receiving.drop_duplicates(subset=['Player'], keep='last', inplace=True)

                write_to_sheet(spreadsheet, f"{prefix}O_Player_Passing", df_passing)
                write_to_sheet(spreadsheet, f"{prefix}O_Player_Rushing", df_rushing)
                write_to_sheet(spreadsheet, f"{prefix}O_Player_Receiving", df_receiving)
        except Exception as e:
            print(f"❌ Could not process Player Stats for {year_to_fetch}: {e}")
            
    print("\n--- Scraping FootballGuys.com Depth Charts ---")
    try:
        url = "https://www.footballguys.com/depth-charts"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
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
            write_to_sheet(spreadsheet, "Depth_Charts", pd.DataFrame(all_players))
    except Exception as e:
        print(f"❌ Could not process Depth Charts: {e}")

    # --- NEW: Fetch Betting Odds ---
    print(f"\n--- Fetching Betting Odds from API ({CURRENT_YEAR}) ---")
    try:
        odds_data = get_api_data("odds", {"league": "1", "season": str(CURRENT_YEAR)})
        parsed_odds_list = []
        
        # Get GameID -> TeamName mapping from the schedule_df we already built
        # This is crucial to know who the spread applies to
        if schedule_df.empty:
             print("  -> ERROR: Schedule data is missing, cannot map odds to teams.")
        else:
            game_to_teams_map = schedule_df.set_index('GameID')[['Home Team', 'Away Team']].to_dict('index')

            for game_odds in odds_data:
                game_id = game_odds.get('game', {}).get('id')
                
                # Find the teams for this game
                team_info = game_to_teams_map.get(str(game_id)) # Use string for lookup
                if not team_info:
                    continue # Skip odds if we don't know the teams for this game

                home_team = team_info['Home Team']
                away_team = team_info['Away Team']

                if not game_odds.get('bookmakers'):
                    continue
                
                # Just take the first bookmaker for simplicity
                bookmaker = game_odds['bookmakers'][0]
                
                spread, over_under, home_spread, away_spread = "N/A", "N/A", "N/A", "N/A"

                for bet in bookmaker.get('bets', []):
                    if bet.get('name') == "Handicap":
                        try:
                            # The API returns two spread values
                            val1 = bet['values'][0]['value']
                            val2 = bet['values'][1]['value']
                            
                            # The 'value' from the API corresponds to the *Home Team* and *Away Team* in that order
                            away_spread = f"{away_team} {val1}"
                            home_spread = f"{home_team} {val2}"
                            
                            # Create a simple spread string
                            if val2.startswith('-'):
                                spread = f"{home_team} {val2}"
                            else:
                                spread = f"{away_team} {val1}"
                        except (IndexError, KeyError):
                            pass # Keep as "N/A"
                    
                    if bet.get('name') == "Total":
                        try:
                            # Find the "Over" value to get the total
                            total_val = next(v['value'] for v in bet['values'] if v['value'].startswith('Over '))
                            over_under = total_val.replace('Over ', '')
                        except (StopIteration, IndexError, KeyError):
                             try:
                                 # Fallback if it's just a number
                                 over_under = bet['values'][0]['value']
                             except (IndexError, KeyError):
                                 pass # Keep as "N/A"

                parsed_odds_list.append({
                    "GameID": game_id,
                    "Home_Spread": home_spread,
                    "Away_Spread": away_spread,
                    "Consensus_Spread": spread,
                    "Over_Under": over_under
                })
        
        if parsed_odds_list:
            odds_df = pd.DataFrame(parsed_odds_list)
            write_to_sheet(spreadsheet, "Betting_Odds", odds_df)
        else:
            print("  -> No odds data was parsed.")

    except Exception as e:
        print(f"❌ Could not process Betting Odds: {e}")

        
    print("\n✅ Scraper script finished.")
