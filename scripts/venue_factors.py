import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from tqdm import tqdm
from dotenv import load_dotenv
from config import Config
from pathlib import Path
import pandas as pd
from collections import defaultdict
from pymongo import MongoClient
from pymongo.operations import UpdateOne

# Constants
PROJECT_PATH = Path(__file__).resolve().parent.parent
MATCH_DATA_DIR = PROJECT_PATH / 'data/raw'

# MongoDB setup
load_dotenv(PROJECT_PATH / '.env')
client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["cricket_elo"]
venue_factors_collection = db["venue_factors"]
processed_matches_collection = db["processed_matches"]

# Ensure indexes exist
if "venue_name_1" not in venue_factors_collection.index_information():
    venue_factors_collection.create_index("venue_name", unique=True)
if "match_id_1" not in processed_matches_collection.index_information():
    processed_matches_collection.create_index("match_id", unique=True)

# Config values
BASE_BATTING_FACTORS = Config.BASE_BATTING_FACTORS
BASE_BOWLING_FACTORS = Config.BASE_BOWLING_FACTORS
ADJUSTMENT_FACTOR = Config.ADJUSTMENT_FACTOR

print('\nVenue Factors Configuration:\n-----------------')
print(f'Base Batting Factors: {BASE_BATTING_FACTORS}')
print(f'Base Bowling Factors: {BASE_BOWLING_FACTORS}')
print(f'Adjustment Factor: {ADJUSTMENT_FACTOR}\n')

def get_match_files():
    """Returns a list of all match file paths in the directory, excluding 'info.csv' and 'all_matches.csv'."""
    return sorted(
        [
            MATCH_DATA_DIR / f
            for f in os.listdir(MATCH_DATA_DIR.resolve())
            if not f.endswith("info.csv") and f != "all_matches.csv" and not f.endswith(".txt")
        ],
        key=lambda x: int(x.name.split(".")[0])
    )

def normalize_factors(venue_stats):
    """Computes adjusted batting and bowling factors using outcome frequencies."""
    total_events = sum(venue_stats.values())
    if total_events == 0:
        return BASE_BATTING_FACTORS.copy(), BASE_BOWLING_FACTORS.copy()

    outcome_probabilities = {outcome: count / total_events for outcome, count in venue_stats.items()}

    batting_factors = BASE_BATTING_FACTORS.copy()
    for outcome in batting_factors:
        if outcome in outcome_probabilities:
            batting_factors[outcome] += ADJUSTMENT_FACTOR * (0.5 - outcome_probabilities[outcome])
            batting_factors[outcome] = max(0, min(1, batting_factors[outcome]))

    bowling_factors = BASE_BOWLING_FACTORS.copy()
    for outcome in bowling_factors:
        if outcome in outcome_probabilities:
            bowling_factors[outcome] += ADJUSTMENT_FACTOR * (0.5 - outcome_probabilities[outcome])
            bowling_factors[outcome] = max(0, min(1, bowling_factors[outcome]))

    # Fix wide and no-ball for all venues
    bowling_factors["wide"] = BASE_BOWLING_FACTORS["wide"]
    bowling_factors["no-ball"] = BASE_BOWLING_FACTORS["no-ball"]

    return batting_factors, bowling_factors

def process_match_file(file_path):
    """Processes a single match file and returns venue, season, and outcome stats."""

    df = pd.read_csv(file_path)
    venue = df["venue"].iloc[0]
    season = str(df["season"].iloc[0])
    venue_stats = defaultdict(int)

    for _, row in df.iterrows():
        if pd.notna(row["wicket_type"]):
            outcome = "wicket"
        elif row["wides"] > 0:
            outcome = "wide"
        elif row["noballs"] > 0:
            outcome = "no-ball"
        else:
            outcome = row["runs_off_bat"]

        venue_stats[outcome] += 1

    return venue, season, venue_stats

def compute_venue_factors_by_season():
    """Computes weighted Elo factors for each venue for each season."""
    all_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # venue → season → outcome → count

    for file in tqdm(get_match_files(), desc="Processing Match Files"):
        result = process_match_file(file)
        if result is None:
            continue
        venue, season, stats = result
        for outcome, count in stats.items():
            all_data[venue][season][outcome] += count

    venue_documents = []

    for venue, season_data in all_data.items():
        batting_factors = {}
        bowling_factors = {}

        seasons_sorted = sorted(season_data.keys())
        for i, season in enumerate(seasons_sorted):
            weighted_stats = defaultdict(float)

            for j in range(i + 1):  # Include previous seasons too
                past_season = seasons_sorted[j]
                weight = 1.5 ** (j - i)  # More recent = higher weight

                for outcome, count in season_data[past_season].items():
                    weighted_stats[outcome] += count * weight

            batting, bowling = normalize_factors(weighted_stats)

            batting_factors[season] = {str(k): v for k, v in batting.items()}
            bowling_factors[season] = {str(k): v for k, v in bowling.items()}

        venue_documents.append(
            UpdateOne(
                {"venue_name": venue},
                {
                    "$set": {
                        "venue_name": venue,
                        "batting_factors": batting_factors,
                        "bowling_factors": bowling_factors
                    }
                },
                upsert=True
            )
        )

    return venue_documents

def update_venue_factors_in_db(venue_operations):
    """Bulk update/inserts venue factors into MongoDB."""
    if venue_operations:
        venue_factors_collection.bulk_write(venue_operations)
        print(f"Inserted or updated {len(venue_operations)} venue documents in MongoDB.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compute seasonal venue Elo factors.")
    parser.add_argument('--force-reprocess', action='store_true', help='Reprocess all matches from scratch.')
    args = parser.parse_args()

    existing = venue_factors_collection.count_documents({})
    if existing > 0:
        if args.force_reprocess:
            print("Clearing previous data and reprocessing all matches...")
            venue_factors_collection.delete_many({})
        else:
            print("Processing only new match files...")

    else:
        print("Processing all match files...")

    operations = compute_venue_factors_by_season()
    update_venue_factors_in_db(operations)
