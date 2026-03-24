import pandas as pd
import os

# 1. SETUP: List all seasons from 18/19 to 24/25
seasons = ['1819', '1920', '2021', '2122', '2223', '2324', '2425']
base_url = "https://www.football-data.co.uk/mmz4281/{}/E0.csv"
all_batches = []

print("--- EPL MULTI-SEASON DATA ACQUISITION START ---")

# 2. ACQUISITION: Download and stack the data
for s in seasons:
    try:
        df = pd.read_csv(base_url.format(s))
        df['Season_Label'] = s  # Identify which season the match belongs to
        all_batches.append(df)
        print(f"  ✓ Season {s} Loaded: {len(df)} matches")
    except Exception as e:
        print(f"  ✗ Error loading season {s}: {e}")

# 3. MERGE: Combine into a SINGLE ENTITY (The Fact Table)
epl_fact_table = pd.concat(all_batches, ignore_index=True)

# 4. DERIVE PRIMARY KEY: Ensure relational integrity (Rubric Requirement)
# Create a unique ID using Date + HomeTeam abbreviation
epl_fact_table['match_id'] = epl_fact_table['Date'].astype(str) + epl_fact_table['HomeTeam'].str[:3]

# 5. EXPORT: Save in both CSV and Parquet (Rubric Requirement)
os.makedirs('data_output', exist_ok=True)
epl_fact_table.to_csv('data_output/epl_match_fact.csv', index=False)
epl_fact_table.to_parquet('data_output/epl_match_fact.parquet', index=False)

print(f"\n--- SUCCESS: Created fact table with {len(epl_fact_table)} total matches ---")
