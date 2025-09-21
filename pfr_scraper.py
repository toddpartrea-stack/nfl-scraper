import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

# --- CONFIGURATION ---
YEAR = 2025

# --- DIAGNOSTIC SCHEDULE SCRAPER ---
def diagnose_schedule(year):
    print("\n--- Starting Diagnostics on PFR SCHEDULE Scrape ---")
    try:
        url = f"https://www.pro-football-reference.com/years/{year}/games.htm"
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table', id='games')
        if not table:
            print("DIAGNOSIS: Could not find the schedule table with id='games'.")
            return

        headers = [th.text.strip() for th in table.find('thead').find_all('th')]
        print(f"Found {len(headers)} headers: {headers}")
        
        # Manually name the blank columns for clarity in our new DataFrame
        headers[5] = 'At'
        headers[7] = 'Boxscore'
        
        rows = []
        # Find all rows in the table body
        table_rows = table.find('tbody').find_all('tr')
        print(f"\nFound {len(table_rows)} total rows in the table body. Analyzing each one...")

        for i, row in enumerate(table_rows):
            print(f"\n--- Analyzing Row {i+1} ---")
            
            # Check for divider rows
            if row.find('th', class_='thead'):
                print(f"Row {i+1} is a divider row. SKIPPING.")
                continue
            
            cols = row.find_all(['th', 'td'])
            print(f"Row {i+1} has {len(cols)} columns.")

            if len(cols) < len(headers):
                print(f"Row {i+1} has fewer columns than expected ({len(cols)} vs {len(headers)}). SKIPPING.")
                continue

            row_data = [ele.text.strip() for ele in cols]
            
            # Check if this is the Thursday game we are looking for
            if row_data[1] == 'Thu' and "September 18" in row_data[2]:
                 print("✅ FOUND THE THURSDAY, SEPTEMBER 18 GAME ROW!")
            
            print(f"Row {i+1} data: {row_data}")
            rows.append(row_data)

        print("\n--- DIAGNOSIS COMPLETE ---")
        print(f"\nSuccessfully processed {len(rows)} game rows out of {len(table_rows)} total rows.")

    except Exception as e:
        print(f"❌ An error occurred during diagnosis: {e}")

# --- MAIN SCRIPT ---
if __name__ == "__main__":
    diagnose_schedule(YEAR)
