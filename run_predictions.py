# --- (Imports and other setup is unchanged) ---
# ...

# --- Main execution block ---
def run_predictions():
    # ... (Authentication, sheet opening, and data loading are the same)
    
    # --- AUTOMATED MATCHUP ANALYSIS ---
    required_tabs = ['Schedule', 'D_Overall', 'Injuries', 'team_match', 'FPI']
    if all(tab in dataframes for tab in required_tabs):
        
        # ... (Clearing old predictions and standardizing team names are the same)
        
        # ... (Finding the current week's games is the same)
        
        for index, game in this_weeks_games.iterrows():
            home_team_full = game['Home_Team']
            away_team_full = game['Away_Team']

            print(f"\n--- Analyzing Matchup: {away_team_full} at {home_team_full} ---")

            # --- Prepare data for the prompt (CORRECTED) ---
            fpi_data = dataframes['FPI']
            # CORRECTED: Looks for the 'Team' column we just created
            home_fpi = fpi_data[fpi_data['Team'].str.contains(home_team_full, case=False, na=False)]
            away_fpi = fpi_data[fpi_data['Team'].str.contains(away_team_full, case=False, na=False)]
            
            team_defense_data = dataframes['D_Overall'] 
            home_def_data = team_defense_data[team_defense_data['Team'] == home_team_full]
            away_def_data = team_defense_data[team_defense_data['Team'] == away_team_full]
            
            injury_data = dataframes['Injuries']
            home_injury_data = injury_data[injury_data['Team'] == home_team_full]
            away_injury_data = injury_data[injury_data['Team'] == away_team_full]
            
            # --- (The prompt is the same) ---
            matchup_prompt = f"""
            Act as an expert NFL analyst. Your task is to predict the outcome of the {away_team_full} at {home_team_full} game.
            Use all the data provided below, especially the ESPN FPI, which is a strong indicator of team strength.

            ---
            ## {home_team_full} (Home) Data
            - **ESPN FPI:** {home_fpi.to_string()}
            - **Defense Stats:** {home_def_data.to_string()}
            - **Injuries:** {home_injury_data.to_string()}

            ## {away_team_full} (Away) Data
            - **ESPN FPI:** {away_fpi.to_string()}
            - **Defense Stats:** {away_def_data.to_string()}
            - **Injuries:** {away_injury_data.to_string()}
            ---

            Based on the structured data above, provide the following in a clear format:
            1. **Predicted Winner:** [Team Name]
            2. **Predicted Final Score:** [Away Team Score] - [Home Team Score]
            3. **Justification:** [A brief justification for your prediction based on the data.]
            """
            
            # ... (The rest of the script is the same)

if __name__ == "__main__":
    run_predictions()