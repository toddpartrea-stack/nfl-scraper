import gspread
import pandas as pd
from datetime import datetime, date
import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# --- CONFIGURATION & AUTH ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
YEAR = 2025
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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

# --- MAIN DIAGNOSTIC SCRIPT ---
def main():
    print("Authenticating with Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    dataframes = {}
    print("\nLoading data from Google Sheet tabs...")
    for worksheet in spreadsheet.worksheets():
        title = worksheet.title
        if not title.startswith("Week_"):
            data = worksheet.get_all_values()
            if data:
                dataframes[title] = pd.DataFrame(data[1:], columns=data[0])
    
    schedule_df_raw = dataframes['Schedule']
    
    print("\n--- STARTING DIAGNOSIS OF SCHEDULE TAB ---")
    print(f"Initial number of rows loaded from Schedule tab: {len(schedule_df_raw)}")
    
    # Check for the Thursday game before any cleaning
    # PFR uses "Thu" for the day and the date is September 18
    tnf_game_before = schedule_df_raw[
        (schedule_df_raw['Day'] == 'Thu') & 
        (schedule_df_raw['Date'].str.contains("September 18", na=False))
    ]
    
    if not tnf_game_before.empty:
        print("\n✅ SUCCESS: Found Thursday game in the raw data before cleaning.")
        print("Raw Thursday Game Row:")
        print(tnf_game_before.to_string())
    else:
        print("\n❌ CRITICAL ERROR: Thursday game is ALREADY MISSING after initial load from the sheet.")
        print("--- DIAGNOSIS COMPLETE ---")
        return

    # --- Step 1: Cleaning the 'Week' column ---
    print("\n--- Analyzing Step 1: Cleaning 'Week' column ---")
    schedule_df_step1 = schedule_df_raw.copy()
    schedule_df_step1['Week_numeric'] = pd.to_numeric(schedule_df_step1['Week'], errors='coerce')
    
    # Check if the Thursday game's week was converted correctly
    tnf_game_after_coerce = schedule_df_step1[
        (schedule_df_step1['Day'] == 'Thu') & 
        (schedule_df_step1['Date'].str.contains("September 18", na=False))
    ]
    print("Thursday game 'Week' value after converting to a number:")
    print(tnf_game_after_coerce[['Week', 'Week_numeric']].to_string())
    
    schedule_df_step1.dropna(subset=['Week_numeric'], inplace=True)
    print(f"Number of rows remaining after dropping rows with invalid Weeks: {len(schedule_df_step1)}")

    tnf_game_after_week_clean = schedule_df_step1[
        (schedule_df_step1['Day'] == 'Thu') & 
        (schedule_df_step1['Date'].str.contains("September 18", na=False))
    ]

    if tnf_game_after_week_clean.empty:
        print("\n🔥 DIAGNOSIS: The Thursday game was DROPPED during the 'Week' column cleaning step.")
        print("This means the value in the 'Week' column for that row is not a valid number.")
        print("--- DIAGNOSIS COMPLETE ---")
        return
    else:
        print("✅ SUCCESS: Thursday game survived 'Week' column cleaning.")

    # --- Step 2: Cleaning the 'Date' column ---
    print("\n--- Analyzing Step 2: Cleaning 'Date' column ---")
    schedule_df_step2 = tnf_game_after_week_clean.copy() # Use the already cleaned data
    schedule_df_step2['game_date'] = pd.to_datetime(schedule_df_step2['Date'] + " " + str(YEAR), errors='coerce').dt.date

    tnf_game_after_date_coerce = schedule_df_step2[
        (schedule_df_step2['Day'] == 'Thu') & 
        (schedule_df_step2['Date'].str.contains("September 18", na=False))
    ]
    print("Thursday game 'Date' value after converting to a date object:")
    print(tnf_game_after_date_coerce[['Date', 'game_date']].to_string())
    
    schedule_df_step2.dropna(subset=['game_date'], inplace=True)
    print(f"Number of rows remaining after dropping rows with invalid Dates: {len(schedule_df_step2)}")
    
    tnf_game_after_date_clean = schedule_df_step2[
        (schedule_df_step2['Day'] == 'Thu') & 
        (schedule_df_step2['Date'].str.contains("September 18", na=False))
    ]

    if tnf_game_after_date_clean.empty:
        print("\n🔥 DIAGNOSIS: The Thursday game was DROPPED during the 'Date' column cleaning step.")
        print("This means the value in the 'Date' column for that row is not a valid date.")
    else:
        print("\n✅ SUCCESS: Thursday game survived 'Date' column cleaning.")
        print("This means the issue is somewhere else in the logic. Please send the log.")

    print("\n--- DIAGNOSIS COMPLETE ---")


if __name__ == "__main__":
    main()
