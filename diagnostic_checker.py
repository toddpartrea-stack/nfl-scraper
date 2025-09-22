import gspread
import pandas as pd
import os
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# --- CONFIGURATION ---
SPREADSHEET_KEY = "1NPpxs5wMkDZ8LJhe5_AC3FXR_shMHxQsETdaiAJifio"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- AUTHENTICATION ---
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

    print("\nLoading Schedule tab...")
    try:
        worksheet = spreadsheet.worksheet("Schedule")
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        
        print("\n--- FINAL DIAGNOSIS ---")
        print("The exact column names found in your 'Schedule' tab are:")
        print(df.columns.tolist())
        print("-----------------------")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()