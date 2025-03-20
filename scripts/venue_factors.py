import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tqdm import tqdm
from dotenv import load_dotenv
from config import Config
from pathlib import Path
import pandas as pd
from collections import defaultdict
from pymongo import MongoClient
from pymongo.operations import UpdateOne

# Define constants
PROJECT_PATH = Path(__file__).resolve().parent.parent
MATCH_DATA_DIR = PROJECT_PATH / 'data/raw'

# MongoDB setup
load_dotenv(PROJECT_PATH / '.env')
client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["cricket_elo"]
venue_factors_collection = db["venue_factors"]

# Add index on venue_name for faster queries
venue_factors_collection.create_index("venue_name", unique=True)

PROJECT_PATH = Path(__file__).resolve().parent.parent
MATCH_DATA_DIR = PROJECT_PATH / 'data/raw'

# Base batting factors (no wide/no-ball)
BASE_BATTING_FACTORS = Config.BASE_BATTING_FACTORS

# Base bowling factors (includes wide and no-ball)
BASE_BOWLING_FACTORS = Config.BASE_BOWLING_FACTORS

# Small adjustment factor to tweak values based on frequency deviation
ADJUSTMENT_FACTOR = Config.ADJUSTMENT_FACTOR

print('\nVenue Factors Configuration:\n-----------------')
print(f'Base Batting Factors: {BASE_BATTING_FACTORS}')
print(f'Base Bowling Factors: {BASE_BOWLING_FACTORS}')
print(f'Adjustment Factor: {ADJUSTMENT_FACTOR}\n')

def get_match_files():
    """Returns a list of all match file paths in the directory."""
    return [MATCH_DATA_DIR / f for f in os.listdir(MATCH_DATA_DIR.resolve()) if not f.endswith("info.csv")]

def process_match_file(file_path):
    """Processes a single match file and extracts venue-specific data."""
    df = pd.read_csv(file_path)
    
    venue = df["venue"].iloc[0]
    venue_stats = defaultdict(int)

    for _, row in df.iterrows():
        outcome = row["runs_off_bat"]
        if pd.notna(row["wicket_type"]):
            outcome = "wicket"
        elif row["wides"] > 0:
            outcome = "wide"
        elif row["noballs"] > 0:
            outcome = "no-ball"
        
        venue_stats[outcome] += 1

    return venue, venue_stats

def normalize_factors(venue_stats):
    """Computes batting and bowling factors based on venue-specific frequencies."""
    total_events = sum(venue_stats.values())
    
    # Compute probabilities of each outcome at the venue
    outcome_probabilities = {outcome: count / total_events for outcome, count in venue_stats.items()}

    # Adjust batting factors
    batting_factors = BASE_BATTING_FACTORS.copy()
    for outcome in batting_factors:
        if outcome in outcome_probabilities:
            batting_factors[outcome] += ADJUSTMENT_FACTOR * (0.5 - outcome_probabilities[outcome])  # Shift towards mean
            batting_factors[outcome] = max(0, min(1, batting_factors[outcome]))  # Clamp between 0-1

    # Adjust bowling factors
    bowling_factors = BASE_BOWLING_FACTORS.copy()
    for outcome in bowling_factors:
        if outcome in outcome_probabilities:
            bowling_factors[outcome] += ADJUSTMENT_FACTOR * (0.5 - outcome_probabilities[outcome])
            bowling_factors[outcome] = max(0, min(1, bowling_factors[outcome]))

    return batting_factors, bowling_factors

def compute_venue_factors():
    """Aggregates stats for all venues and normalizes to compute Elo factors."""
    venue_data = {}

    for file in tqdm(get_match_files(), desc='Processing Data Files'):
        try:
            venue, stats = process_match_file(file)
            batting_factors, bowling_factors = normalize_factors(stats)

            # Convert integer keys to strings for MongoDB compatibility
            batting_factors = {str(k): v for k, v in batting_factors.items()}
            bowling_factors = {str(k): v for k, v in bowling_factors.items()}

            # Fixing wide and no-ball factors for all venues
            bowling_factors['no-ball'] = BASE_BOWLING_FACTORS['no-ball']
            bowling_factors['wide'] = BASE_BOWLING_FACTORS['wide']

            venue_data[venue] = {
                "venue_name": venue,
                "batting_factors": batting_factors,
                "bowling_factors": bowling_factors
            }
        except Exception as e:
            print(f'Error processing match file {file}: {e}')

    return venue_data

def update_venue_factors_in_db(venue_factors):
    """Updates or inserts venue factors into MongoDB."""
    bulk_operations = []

    for venue_name, data in venue_factors.items():
        bulk_operations.append(
            UpdateOne(
                {"venue_name": venue_name},
                {"$set": data},
                upsert=True
            )
        )

    if bulk_operations:
        venue_factors_collection.bulk_write(bulk_operations)
        print(f"inserted venue factor documents in MongoDB.")

if __name__ == '__main__':
    venue_factors = compute_venue_factors()
    update_venue_factors_in_db(venue_factors)