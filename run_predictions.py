import os
import json
import re
import time
import pandas as pd
import pytz
import gspread
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import vertexai
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
from gspread_formatting import CellFormat, format_cell_range

load_dotenv()

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
FOOTBALL_API_KEY = os.getenv('AMERICAN_FOOTBALL_API_KEY')
FOOTBALL_API_HOST = "v1.american-football.api-sports.io"
# No weather API key needed!
YEAR = 2025
MANUAL_WEEK_OVERRIDE = None

# --- TEAM LOCATION MAP (Latitude/Longitude) ---
# Used as the coordinate source for the NWS API
TEAM_LOCATION_MAP = {
    "Arizona Cardinals": {"lat": 33.5276, "lon": -112.2626},
    "Atlanta Falcons": {"lat": 33.7554, "lon": -84.4009},
    "Baltimore Ravens": {"lat": 39.2780, "lon": -76.6227},
    "Buffalo Bills": {"lat": 42.7738, "lon": -78.7869},
    "Carolina Panthers": {"lat": 35.2259, "lon": -80.8529},
    "Chicago Bears": {"lat": 41.8623, "lon": -87.6167},
    "Cincinnati Bengals": {"lat": 39.0955, "lon": -84.5160},
    "Cleveland Browns": {"lat": 41.5061, "lon": -81.6995},
    "Dallas Cowboys": {"lat": 32.7479, "lon": -97.0928},
    "Denver Broncos": {"lat": 39.7439, "lon": -105.0201},
    "Detroit Lions": {"lat": 42.3400, "lon": -83.0456},
    "Green Bay Packers": {"lat": 44.5013, "lon": -88.0622},
    "Houston Texans": {"lat": 29.6847, "lon": -95.4109},
    "Indianapolis Colts": {"lat": 39.7601, "lon": -86.1639},
    "Jacksonville Jaguars": {"lat": 30.3239, "lon": -81.6373},
    "Kansas City Chiefs": {"lat": 39.0489, "lon": -94.4839},
    "Las Vegas Raiders": {"lat": 36.0908, "lon": -115.1837},
    "Los Angeles Chargers": {"lat": 33.9534, "lon": -118.3390},
    "Los Angeles Rams": {"lat": 33.9534, "lon": -118.3390},
    "Miami Dolphins": {"lat": 25.9580, "lon": -80.2389},
    "Minnesota Vikings": {"lat": 44.9736, "lon": -93.2580},
    "New England Patriots": {"lat": 42.0909, "lon": -71.2643},
    "New Orleans Saints": {"lat": 29.9509, "lon": -90.0812},
    "New York Giants": {"lat": 40.8135, "lon": -74.0745},
    "New York Jets": {"lat": 40.8135, "lon": -74.0745},
    "Philadelphia Eagles": {"lat": 39.9008, "lon": -75.1675},
    "Pittsburgh Steelers": {"lat": 40.4467, "lon": -80.0158},
    "San Francisco 49ers": {"lat": 37.4032, "lon": -121.9697},
    "Seattle Seahawks": {"lat": 47.5952, "lon": -122.3316},
    "Tampa Bay Buccaneers": {"lat": 27.9759, "lon": -82.5033},
    "Tennessee Titans": {"lat": 36.1665, "lon": -86.7713},
    "Washington Commanders": {"lat": 38.9077, "lon": -76.8645}
}


# --- AUTHENTICATION & HELPERS ---
def get_gspread_client():
    credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_path:
        raise ValueError("Could not find Google credentials path. The auth step in the workflow may have failed.")
    return gspread.service_account(filename=credential_path)

def normalize_player_name(name):
    """Removes suffixes (Jr, Sr, II, etc.) and punctuation for reliable matching."""
    if not isinstance(name, str):
        return name
    name = re.sub(r'\s+(Jr|Sr|II|III|IV|V)\.?$', '', name, flags=re.IGNORECASE)
    name = name.replace('.', '')
    return name.strip()

def get_api_data(endpoint, params):
    # This is for the football API
    url = f"https://{FOOTBALL_API_HOST}/{endpoint}"
    headers = {"x-rapidapi-key": FOOTBALL_API_KEY, "x-rapidapi-host": FOOTBALL_API_HOST}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get('response', [])
    except requests.exceptions.RequestException as e:
        print(f"  -> API request failed for endpoint '{endpoint}': {e}")
        return []

# --- NEW: NWS WEATHER HELPER FUNCTION (FREE, NO KEY) ---
def get_weather_forecast(city, country, home_team, game_datetime_utc):
    """
    Fetches weather for the game's specific venue using the free NWS API.
    NWS API is US-Only, so it checks country first.
    Falls back to home team's default stadium coordinates.
    """
    
    # NWS API is US-only. Check for known non-US countries.
    if country and country.lower() not in ["united states", "usa", "us", "n/a", "", None]:
        return f"Weather not available (non-US game: {city}, {country})"
    
    # Check if game is within the 7-day forecast window
    now_utc = datetime.now(timezone.utc)
    days_until_game = (game_datetime_utc.date() - now_utc.date()).days
    
    if days_until_game < 0:
        return "Game has already passed."
    if days_until_game > 6: # NWS provides 7 days, so 0-6 is valid
        return "Forecast not yet available (game is >7 days away)."

    # Get coordinates from our fallback map
    if home_team not in TEAM_LOCATION_MAP:
        return f"Weather not available (team '{home_team}' not in US stadium map)."
        
    coords = TEAM_LOCATION_MAP[home_team]
    lat, lon = coords['lat'], coords['lon']
    
    # NWS API requires a User-Agent.
    # You can customize this if you want, but a generic one is fine.
    headers = {
        "User-Agent": "NFL-Prediction-Script (github.com/google/generative-ai-docs)"
    }
    
    try:
        # --- Step 1: Get the gridpoint URL from NWS ---
        # This tells us *which* NWS office covers this lat/lon
        points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
        response = requests.get(points_url, headers=headers, timeout=10)
        response.raise_for_status()
        grid_url = response.json()['properties']['forecast']
        
        # --- Step 2: Get the actual forecast from that grid URL ---
        response = requests.get(grid_url, headers=headers, timeout=10)
        response.raise_for_status()
        forecast_data = response.json()
        
        # --- Step 3: Parse the forecast periods to find the right day ---
        game_date = game_datetime_utc.date()
        
        # NWS gives day/night periods. We need to find the one that matches our game time.
        # We'll look for the first period that starts ON the game day.
        for period in forecast_data['properties']['periods']:
            period_start_time = datetime.fromisoformat(period['startTime'])
            
            if period_start_time.date() == game_date:
                # Found the first period for the game day. This is our forecast.
                period_name = period['name'] # e.g., "Sunday" or "Sunday Night"
                temp = period['temperature']
                wind_speed = period['windSpeed']
                short_forecast = period['shortForecast']
                
                return (
                    f"Forecast for {city} ({period_name}): {short_forecast}. "
                    f"Temp: {temp}°{period.get('temperatureUnit', 'F')}. "
                    f"Wind: {wind_speed}."
                )
                
        return "Forecast data not found for game day."

    except requests.exceptions.RequestException as e:
        print(f"  -> NWS Weather API request failed: {e}")
        return "Error fetching NWS weather data."
    except Exception as e:
        print(f"  -> Error processing NWS weather data: {e}")
        return "Error processing NWS weather."


def get_top_healthy_player_names(team_df, position, num_players=1):
    position_df = team_df[team_df['Position'] == position]
    healthy_df = position_df[position_df['Status'] == 'Healthy']
    players = healthy_df.head(num_players)['Player'].tolist()
    while len(players) < num_players:
        players.append("[Not Available]")
    return players

def find_or_create_row(worksheet, away_team, home_team, kickoff_str):
    all_sheet_data = worksheet.get_all_values()
    for i, row in enumerate(all_sheet_data[1:], start=2):
        if row and len(row) > 1 and row[0].strip() == away_team and row[1].strip() == home_team:
            return i
    worksheet.append_row([away_team, home_team, kickoff_str, '', '', ''])
    return len(all_sheet_data) + 1

def hide_data_sheets(spreadsheet):
    print("\n--- Cleaning up spreadsheet visibility ---")
    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if sheet.title.startswith("Week_") or sheet.title == "Todds Tab":
            try: sheet.show()
            except Exception: pass
        else:
            try: sheet.hide()
            except Exception: pass

def clean_json_response(text):
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        return match.group(1)
    return text.strip()

def run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=None):
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']
    
    current_week = 0
    if week_override:
        current_week = week_override
    else:
        future_games = schedule_df[schedule_df['datetime'] > now_utc]
        if not future_games.empty:
            current_week = int(future_games['Week'].min())

    if not current_week:
        print("  -> No future games found to predict.")
        return

    print(f"  -> Generating predictions for Week {current_week}")
    sheet_name = f"Week_{current_week}_Predictions"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        time.sleep(1)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=6)
    
    headers = ["Away Team", "Home Team", "Kickoff", "Predicted Winner", "Predicted Score", "Prediction Analysis"]
    worksheet.update('A1', [headers])
    worksheet.freeze(rows=1)
    fmt = CellFormat(wrapStrategy='WRAP')
    format_cell_range(worksheet, 'F:F', fmt)

    this_weeks_games = schedule_df[schedule_df['Week'] == current_week]
    
    print("--- Initializing Vertex AI ---")
    vertexai.init()
    model = GenerativeModel("gemini-2.5-pro")
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    }
    
    depth_chart_df = dataframes.get('Depth_Charts')
    player_stats_current = dataframes.get('player_stats_current')
    team_offense_df = dataframes.get('O_Team_Overall')

    for index, game in this_weeks_games.iterrows():
        away_team_full, home_team_full = game['Away Team'], game['Home Team']
        game_time_utc = game['datetime']
        
        # --- Get venue from schedule ---
        venue_city = game.get('Venue_City', 'N/A')
        venue_country = game.get('Venue_Country', 'N/A')
        
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        print(f"  -> Venue: {venue_city}, {venue_country}")
        
        kickoff_display_str = game_time_utc.astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        # --- Get live weather forecast using NWS API ---
        print("  -> Fetching live weather forecast...")
        weather_forecast_str = get_weather_forecast(
            venue_city, 
            venue_country, 
            home_team_full, # Used for lat/lon lookup
            game_time_utc
        )
        print(f"  -> Weather: {weather_forecast_str}")
        # --- END NEW ---

        home_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == home_team_full]
        away_depth_chart = depth_chart_df[depth_chart_df['Team_Full'] == away_team_full]

        pos_config = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1}
        home_player_names = {p for pos, num in pos_config.items() for p in get_top_healthy_player_names(home_depth_chart, pos, num) if p != "[Not Available]"}
        away_player_names = {p for pos, num in pos_config.items() for p in get_top_healthy_player_names(away_depth_chart, pos, num) if p != "[Not Available]"}
        
        home_player_names_normalized = {normalize_player_name(p) for p in home_player_names}
        away_player_names_normalized = {normalize_player_name(p) for p in away_player_names}
        
        home_roster_stats = player_stats_current[player_stats_current['Player_Normalized'].isin(home_player_names_normalized)]
        away_roster_stats = player_stats_current[player_stats_current['Player_Normalized'].isin(away_player_names_normalized)]

        # --- UPDATED PROMPT ---
        matchup_prompt = f"""
        You are an expert sports analyst and data scientist. Your task is to provide a detailed prediction analysis for an upcoming NFL game.
        **Your primary directive is to base your analysis exclusively on the data provided below. Do not use any prior knowledge.**
        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home).
        You MUST factor in the provided weather forecast, especially if it indicates high winds, rain, or snow, which typically favors the running game and defense.
        If the weather forecast is not available, proceed with the analysis based on the player and team stats alone.

        ## Data for Analysis:
        ### Team Standings ({YEAR}):
        {team_offense_df[team_offense_df['Team_Full'].isin([home_team_full, away_team_full])].to_string()}
        
        ### Weather Forecast:
        {weather_forecast_str}

        ### Home Team - Healthy Player Stats ({YEAR}):
        {home_roster_stats.to_string()}

        ### Away Team - Healthy Player Stats ({YEAR}):
        {away_roster_stats.to_string()}
        ---
        Based on your analysis of ONLY the data provided, provide your complete response as a single, valid JSON object with no markdown.
        Your response must contain keys for "game_prediction", "justification", "top_performers", and "touchdown_scorers".
        - In "top_performers", identify the 3-4 most impactful offensive players from EACH team.
        - For every 'confidence' field, you MUST provide an integer between 1 and 100.

        Example JSON schema:
        {{
          "game_prediction": {{ "winner": "string", "winner_confidence": 85, "score": "string", "score_confidence": 70 }},
          "justification": "string",
          "top_performers": [
              {{ "player_name": "string", "team": "string", "predicted_stats": {{ "Passing Yards": 250, "Passing Yards_confidence": 65 }} }},
              {{ "player_name": "string", "team": "string", "predicted_stats": {{ "Rushing Yards": 80, "Rushing Yards_confidence": 70 }} }}
          ],
          "touchdown_scorers": [ {{ "player_name": "string", "confidence": 75 }} ]
        }}
        """
        # --- END UPDATED PROMPT ---

        try:
            response = model.generate_content(matchup_prompt, safety_settings=safety_settings)
            pred_json = json.loads(clean_json_response(response.text))

            game_pred = pred_json.get("game_prediction", {})
            winner = game_pred.get("winner", "N/A")
            score = game_pred.get("score", "N/A")
            
            justification = pred_json.get("justification", "No justification provided.")
            top_performers = pred_json.get("top_performers", [])
            td_scorers = pred_json.get("touchdown_scorers", [])

            analysis_text = f"**1. Game Prediction:**\n"
            analysis_text += f"***Predicted Winner:** {winner} (Confidence: {game_pred.get('winner_confidence', 0)}%)\n"
            analysis_text += f"***Predicted Final Score:** {score} (Confidence: {game_pred.get('score_confidence', 0)}%)\n\n"
            
            analysis_text += f"**2. Top Performer Stat Predictions:**\n"
            if not top_performers:
                analysis_text += "No key performers identified.\n\n"
            else:
                for player in top_performers:
                    stats = player.get("predicted_stats", {})
                    p_text = f"***{player.get('player_name', 'N/A')} ({player.get('team', 'N/A')}):**\n"
                    if 'Passing Yards' in stats: p_text += f"** Passing Yards:** {stats.get('Passing Yards', 'N/A')} (Confidence: {stats.get('Passing Yards_confidence', 0)}%)\n"
                    if 'Rushing Yards' in stats: p_text += f"** Rushing Yards:** {stats.get('Rushing Yards', 'N/A')} (Confidence: {stats.get('Rushing Yards_confidence', 0)}%)\n"
                    if 'Receiving Yards' in stats: p_text += f"** Receiving Yards:** {stats.get('Receiving Yards', 'N/A')} (Confidence: {stats.get('Receiving Yards_confidence', 0)}%)\n"
                    if 'Passing TDs' in stats: p_text += f"** Passing TDs:** {stats.get('Passing TDs', 'N/A')} (Confidence: {stats.get('Passing TDs_confidence', 0)}%)\n"
                    if 'Rushing TDs' in stats: p_text += f"** Rushing TDs:** {stats.get('Rushing TDs', 'N/A')} (Confidence: {stats.get('Rushing TDs_confidence', 0)}%)\n"
                    if 'Receiving TDs' in stats: p_text += f"** Receiving TDs:** {stats.get('Receiving TDs', 'N/A')} (Confidence: {stats.get('Receiving TDs_confidence', 0)}%)\n"
                    if 'Interceptions' in stats: p_text += f"** Interceptions:** {stats.get('Interceptions', 'N/A')} (Confidence: {stats.get('Interceptions_confidence', 0)}%)\n"
                    analysis_text += p_text + "\n"

            analysis_text += f"**3. Touchdown Scorers:**\n"
            for scorer in td_scorers:
                player_name = scorer.get("player_name", "N/A")
                confidence = scorer.get("confidence", 0)
                analysis_text += f"** {player_name} (Confidence: {confidence}%)\n"
            analysis_text += "\n"

            analysis_text += f"**4. Justification:**\n{justification}"

            worksheet.update(f'D{row_num}:F{row_num}', [[winner, score, analysis_text.strip()]])
            print(f"    -> SUCCESS: Wrote formatted prediction for {away_team_full} vs {home_team_full}")
        except Exception as e:
            print(f"    -> ERROR: Could not generate or parse prediction: {e}")
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                print(f"    -> AI Response Finish Reason: {response.candidates[0].finish_reason}")
                print(f"    -> AI Response Safety Ratings: {response.candidates[0].safety_ratings}")
        time.sleep(5)

def main():
    if not FOOTBALL_API_KEY:
        print("❌ CRITICAL ERROR: AMERICAN_FOOTBALL_API_KEY secret not found.")
        return
    
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
    
    dataframes = {}
    print("\nLoading all data from Google Sheet tabs...")
    sheet_titles = [s.title for s in spreadsheet.worksheets()]
    for title in sheet_titles:
        if not title.startswith("Week_"):
            print(f"  -> Loading '{title}'...")
            worksheet = spreadsheet.worksheet(title)
            data = worksheet.get_all_values()
            if data and len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                if 'Player' in df.columns:
                    df['Player_Normalized'] = df['Player'].apply(normalize_player_name)
                dataframes[title] = df

    print("\n--- Unifying Team Names Across All Data Sources ---")
    if 'team_match' not in dataframes:
        print("❌ CRITICAL ERROR: 'team_match' tab not found. Cannot unify team names.")
        return
        
    team_map_df = dataframes['team_match']
    master_team_map = {row[col]: row['Full Name'] for _, row in team_map_df.iterrows() for col in team_map_df.columns if pd.notna(row[col]) and row[col]}
    
    team_name_columns = ['Tm', 'Team', 'Away Team', 'Home Team']
    for name, df in dataframes.items():
        for col in team_name_columns:
            if col in df.columns:
                if col == 'Tm' or col == 'Team':
                    df['Team_Full'] = df[col].map(master_team_map).fillna(df[col])
                else:
                    df[col] = df[col].map(master_team_map).fillna(df[col])
        dataframes[name] = df
    
    player_stat_dfs = []
    for sheet_name in ['O_Player_Passing', 'O_Player_Rushing', 'O_Player_Receiving']:
        if sheet_name in dataframes:
            player_stat_dfs.append(dataframes[sheet_name])
    
    if not player_stat_dfs:
        print("❌ CRITICAL ERROR: No player stats tabs (O_Player_Passing, etc.) found.")
        return
        
    dataframes['player_stats_current'] = pd.concat(player_stat_dfs, ignore_index=True)


    eastern_tz = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    
    if 'Schedule' not in dataframes:
        print("❌ CRITICAL ERROR: 'Schedule' tab not found. Cannot determine games to predict.")
        return
        
    schedule_df = dataframes['Schedule']
    
    # Check for the schedule columns we need for weather
    if 'Venue_Country' not in schedule_df.columns:
        print("❌ CRITICAL ERROR: 'Schedule' tab is missing 'Venue_Country'.")
        print("  -> Please re-run the 'pfr_scraper.py' script from our previous conversation to update the sheet.")
        return
    
    schedule_df = schedule_df[schedule_df['Date'] != 'Date'].copy()
    datetime_str = schedule_df['Date'] + " " + schedule_df['Time']
    schedule_df['datetime'] = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M', errors='coerce').dt.tz_localize('UTC')
    schedule_df.dropna(subset=['datetime'], inplace=True)
    schedule_df['Week'] = pd.to_numeric(schedule_df['Week'], errors='coerce')
    schedule_df.dropna(subset=['Week'], inplace=True)
    schedule_df['Week'] = schedule_df['Week'].astype(int)
    dataframes['Schedule'] = schedule_df

    print("\n--- Running PREDICTION mode for upcoming week. ---")
    run_prediction_mode(spreadsheet, dataframes, now_utc, week_override=MANUAL_WEEK_OVERRIDE)

    hide_data_sheets(spreadsheet)
    print("\n✅ Prediction/Results script finished.")

if __name__ == "__main__":
    main()
