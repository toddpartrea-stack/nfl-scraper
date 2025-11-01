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
YEAR = 2025
MANUAL_WEEK_OVERRIDE = None

# --- TEAM LOCATION MAP (Latitude/Longitude) ---
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

# --- NWS WEATHER HELPER FUNCTION (FREE, NO KEY) ---
def get_weather_forecast(city, country, home_team, game_datetime_utc):
    """
    Fetches weather for the game's specific venue using the free NWS API.
    NWS API is US-Only, so it checks country first.
    Falls back to home team's default stadium coordinates.
    """
    
    if country and country.lower() not in ["united states", "usa", "us", "n/a", "", None]:
        return f"Weather not available (non-US game: {city}, {country})"
    
    now_utc = datetime.now(timezone.utc)
    days_until_game = (game_datetime_utc.date() - now_utc.date()).days
    
    if days_until_game < 0:
        return "Game has already passed."
    if days_until_game > 6:
        return "Forecast not yet available (game is >7 days away)."

    if home_team not in TEAM_LOCATION_MAP:
        return f"Weather not available (team '{home_team}' not in US stadium map)."
        
    coords = TEAM_LOCATION_MAP[home_team]
    lat, lon = coords['lat'], coords['lon']
    
    headers = {
        "User-Agent": "NFL-Prediction-Script (github.com/google/generative-ai-docs)"
    }
    
    try:
        points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
        response = requests.get(points_url, headers=headers, timeout=10)
        response.raise_for_status()
        grid_url = response.json()['properties']['forecast']
        
        response = requests.get(grid_url, headers=headers, timeout=10)
        response.raise_for_status()
        forecast_data = response.json()
        
        game_date = game_datetime_utc.date()
        
        for period in forecast_data['properties']['periods']:
            period_start_time = datetime.fromisoformat(period['startTime'])
            
            if period_start_time.date() == game_date:
                period_name = period['name']
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

# --- UPDATED HIDE SHEETS FUNCTION ---
def hide_data_sheets(spreadsheet, current_week):
    print("\n--- Cleaning up spreadsheet visibility ---")
    
    # Define which sheets should ALWAYS be visible
    latest_pred_sheet = f"Week_{current_week}_Predictions"
    always_visible = [latest_pred_sheet, "Betting_Odds", "Todds Tab"]
    
    # If no week is active, 0 is passed, so the pred sheet won't match
    if current_week == 0:
        always_visible = ["Betting_Odds", "Todds Tab"]

    sheets = spreadsheet.worksheets()
    for sheet in sheets:
        if sheet.title in always_visible:
            try:
                sheet.show()
                print(f"  -> Showing '{sheet.title}'")
            except Exception: pass
        else:
            try:
                sheet.hide()
                # print(f"  -> Hiding '{sheet.title}'") # Optional: for debugging
            except Exception: pass

def clean_json_response(text):
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        return match.group(1)
    return text.strip()

def run_prediction_mode(spreadsheet, dataframes, now_utc, current_week):
    eastern_tz = pytz.timezone('US/Eastern')
    schedule_df = dataframes['Schedule']
    
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
        
        venue_city = game.get('Venue_City', 'N/A')
        venue_country = game.get('Venue_Country', 'N/A')
        
        print(f"\n--- Predicting: {away_team_full} at {home_team_full} ---")
        print(f"  -> Venue: {venue_city}, {venue_country}")
        
        kickoff_display_str = game_time_utc.astimezone(eastern_tz).strftime('%Y-%m-%d %I:%M %p %Z')
        row_num = find_or_create_row(worksheet, away_team_full, home_team_full, kickoff_display_str)
        
        # --- Get Weather ---
        print("  -> Fetching live weather forecast...")
        weather_forecast_str = get_weather_forecast(
            venue_city, venue_country, home_team_full, game_time_utc
        )
        print(f"  -> Weather: {weather_forecast_str}")

        # --- NEW: Get Betting Odds ---
        spread = game.get('Consensus_Spread', 'N/A')
        over_under = game.get('Over_Under', 'N/A')
        betting_str = f"Spread: {spread} | Over/Under: {over_under}"
        print(f"  -> Odds: {betting_str}")
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

        # --- UPDATED PROMPT (with Weather Rules AND Betting) ---
        matchup_prompt = f"""
        You are an expert sports analyst and data scientist. Your task is to provide a detailed prediction analysis for an upcoming NFL game.
        **Your primary directive is to base your analysis exclusively on the data provided below. Do not use any prior knowledge.**
        Analyze the matchup between the {away_team_full} (Away) and {home_team_full} (Home).
        
        ## AI Analysis Directives:
        You MUST follow these rules when analyzing the data:
        1.  **WEATHER:**
            * **HIGH WIND (20+ mph):** This is the most significant factor. High winds severely NEGATIVELY impact passing yards, passing accuracy (especially deep throws), and all kicking. High wind STRONGLY favors the running game and defense.
            * **RAIN / SNOW:** These conditions make the ball slippery, increasing fumbles and dropped passes. This NEGATIVELY impacts passing offenses and favors teams with a strong running game.
            * **FAVORABLE WEATHER (Dome, or <10 mph wind and no rain/snow):** This heavily favors passing offenses.
        2.  **BETTING MARKET:**
            * The Spread and Over/Under are a strong signal of the expected game script and outcome.
            * You MUST factor this into your analysis (e.g., "The market expects a close, low-scoring game").
        3.  **LOGIC:** You must explicitly state how the weather and betting markets are influencing your prediction, especially if they contradict (e.g., "The stats favor Team A, but the high winds neutralize their passing attack, which is why I'm picking Team B").

        ## Data for Analysis:
        ### 1. Betting Market Consensus:
        {betting_str}

        ### 2. Team Standings ({YEAR}):
        {team_offense_df[team_offense_df['Team_Full'].isin([home_team_full, away_team_full])].to_string()}
        
        ### 3. Weather Forecast:
        {weather_forecast_str}

        ### 4. Home Team - Healthy Player Stats ({YEAR}):
        {home_roster_stats.to_string()}

        ### 5. Away Team - Healthy Player Stats ({YEAR}):
        {away_roster_stats.to_string()}
        ---
        Based on your analysis of ONLY the data provided (including the AI Analysis Directives), provide your complete response as a single, valid JSON object with no markdown.
        Your response must contain keys for "game_prediction", "justification", "top_performers", and "touchdown_scorers".
        - In "justification", you must explain HOW the betting odds AND weather forecast impacted your prediction.
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
                    if 'Interceptions' in stats: p_text += f"** Interceptions:** {stats.get('Interceptions', 'N/A')} (Confidence: {stats.get('Interceptions_confidence',
