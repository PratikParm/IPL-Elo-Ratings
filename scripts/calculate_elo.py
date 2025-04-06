import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from tqdm import tqdm
from dotenv import load_dotenv
from config import Config
from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from pymongo.operations import UpdateOne
from datetime import datetime, timedelta

# Define constants
PROJECT_PATH = Path(__file__).resolve().parent.parent
MATCH_DATA_DIR = PROJECT_PATH / 'data/raw'

# MongoDB setup
load_dotenv(PROJECT_PATH / '.env')
client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["cricket_elo"]
venue_factors_collection = db["venue_factors"]
player_ratings_collection = db["player_ratings"]
processed_matches_collection = db["processed_matches"]

# Create index on player_name if it doesn't exist
existing_player_indexes = player_ratings_collection.index_information()
if "player_name_1" not in existing_player_indexes:
    player_ratings_collection.create_index([("player_name", 1)])

# Create index on match_id if it doesn't exist
existing_match_indexes = processed_matches_collection.index_information()
if "match_id_1" not in existing_match_indexes:
    processed_matches_collection.create_index("match_id", unique=True)

# Elo Configuration
DEFAULT_ELO = Config.DEFAULT_ELO
K_FACTOR = Config.K_FACTOR
DECAY_TIME_THRESHOLD = Config.DECAY_TIME_THRESHOLD
DECAY_RATE = Config.DECAY_RATE

print('\nELO Configuration:\n-----------------')
print(f'Default Elo: {DEFAULT_ELO}')
print(f'K Factor: {K_FACTOR}')
print(f'Decay Time Threshold: {DECAY_TIME_THRESHOLD}')
print(f'Decay Rate: {DECAY_RATE}\n')

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


def get_venue_factors(venue_name):
    """Fetches venue-specific outcome factors from MongoDB."""
    venue_doc = venue_factors_collection.find_one({"venue_name": venue_name})
    if venue_doc:
        return venue_doc["batting_factors"], venue_doc["bowling_factors"]
    else:
        print(f"Warning: No venue factors found for {venue_name}, using defaults.")
        return None, None


def get_latest_player_rating(player_name):
    """Fetches the latest Elo rating of a player, or initializes it if missing."""
    player_doc = player_ratings_collection.find_one({"player_name": player_name})
    if player_doc:
        batting_ratings = player_doc.get("batting_rating", [])
        bowling_ratings = player_doc.get("bowling_rating", [])

        last_batting_rating = batting_ratings[-1] if batting_ratings else {"date": None, "rating": DEFAULT_ELO}
        last_bowling_rating = bowling_ratings[-1] if bowling_ratings else {"date": None, "rating": DEFAULT_ELO}

        return last_batting_rating, last_bowling_rating

    return {"date": None, "rating": DEFAULT_ELO}, {"date": None, "rating": DEFAULT_ELO}


def expected_outcome(player_rating, opponent_rating):
    """Calculates the expected outcome probability using the Elo formula."""
    return 1 / (1 + 10 ** ((opponent_rating - player_rating) / 400))


def update_player_ratings(player_name, match_date, batting_rating=None, bowling_rating=None):
    """
    Updates the player's Elo ratings in MongoDB.
    - Appends new batting/bowling ratings at the end of the list.
    - Updates only one type of rating per call (either batting or bowling).
    """
    if batting_rating is not None:
        player_ratings_collection.update_one(
            {"player_name": player_name},
            {
                "$push": {"batting_rating": {"date": match_date, "rating": batting_rating}}
            },
            upsert=True
        )

    elif bowling_rating is not None:
        player_ratings_collection.update_one(
            {"player_name": player_name},
            {
                "$push": {"bowling_rating": {"date": match_date, "rating": bowling_rating}}
            },
            upsert=True
        )


def apply_seasonal_decay(first_match_date_of_season):
    """Applies Elo decay for players who were inactive for over a year before a new season starts."""
    season_start_dt = datetime.strptime(first_match_date_of_season, "%Y-%m-%d")
    one_year_ago = season_start_dt - timedelta(days=DECAY_TIME_THRESHOLD)

    # Query all players
    all_players = player_ratings_collection.find({}, {"player_name": 1, "batting_rating": 1, "bowling_rating": 1})

    for player in all_players:
        player_name = player["player_name"]
        batting_ratings = player.get("batting_rating", [])
        bowling_ratings = player.get("bowling_rating", [])

        if not batting_ratings and not bowling_ratings:
            continue  # Skip players with no rating history

        last_batting = batting_ratings[-1] if batting_ratings else {"date": None, "rating": DEFAULT_ELO}
        last_bowling = bowling_ratings[-1] if bowling_ratings else {"date": None, "rating": DEFAULT_ELO}     
        

        if last_batting.get("date") is not None:
            last_batted_date = datetime.strptime(last_batting["date"], "%Y-%m-%d")

            if last_batted_date < one_year_ago:

                # Apply decay
                new_batting_rating = last_batting["rating"] - DECAY_RATE

                # Update decayed ratings in DB
                update_player_ratings(player_name, first_match_date_of_season, batting_rating=new_batting_rating, bowling_rating=None)

        if last_bowling.get("date") is not None:
            last_bowled_date = datetime.strptime(last_bowling["date"], "%Y-%m-%d")

            if last_bowled_date < one_year_ago:
                # Apply decay
                new_bowling_rating = last_bowling["rating"] - DECAY_RATE

                # Update decayed ratings in DB
                update_player_ratings(player_name, first_match_date_of_season, batting_rating=None, bowling_rating=new_bowling_rating)


def process_match_file(file_path):
    """Processes a match file and updates player Elo ratings efficiently."""
    match_id = file_path.stem.split(".")[0]
    if processed_matches_collection.find_one({"match_id": match_id}):
        return
    
    processed_matches_collection.insert_one({"match_id": match_id})
    df = pd.read_csv(file_path)
    venue_name = df["venue"].iloc[0]
    match_date = df["start_date"].iloc[0]

    # Fetch venue-specific factors
    batting_factors, bowling_factors = get_venue_factors(venue_name)
    if batting_factors is None or bowling_factors is None:
        return

    # Preload all player ratings
    players = set(df["striker"]).union(set(df["bowler"]))
    player_ratings = {player: {} for player in players}  # Store only needed ratings

    # Fetch latest ratings in one query
    rating_docs = player_ratings_collection.find({"player_name": {"$in": list(players)}})
    for doc in rating_docs:
        player_name = doc["player_name"]
        if "batting_rating" in doc:
            player_ratings[player_name]["batting"] = doc["batting_rating"][-1]["rating"]
        if "bowling_rating" in doc:
            player_ratings[player_name]["bowling"] = doc["bowling_rating"][-1]["rating"]

    # Default Elo for new players
    for player in players:
        if "batting" not in player_ratings[player]:
            player_ratings[player]["batting"] = DEFAULT_ELO
        if "bowling" not in player_ratings[player]:
            player_ratings[player]["bowling"] = DEFAULT_ELO

    # Process each ball
    for _, row in df.iterrows():
        if row["wicket_type"] == "run out":
            continue  # Skip run outs

        batsman = row["striker"]
        bowler = row["bowler"]
        runs = row["runs_off_bat"]

        # Determine outcome
        if pd.notna(row["wicket_type"]):
            outcome = "wicket"
        elif row["wides"] > 0:
            outcome = "wide"
        elif row["noballs"] > 0:
            outcome = "no-ball"
        else:
            outcome = str(runs)

        # Get expected outcomes
        E_batsman = expected_outcome(player_ratings[batsman]["batting"], player_ratings[bowler]["bowling"])
        E_bowler = expected_outcome(player_ratings[bowler]["bowling"], player_ratings[batsman]["batting"])

        # Get actual outcome factor (S)
        S_batsman = batting_factors.get(outcome, 0.5)
        S_bowler = bowling_factors.get(outcome, 0.5)

        # Update ratings
        player_ratings[batsman]["batting"] += K_FACTOR * (S_batsman - E_batsman)
        player_ratings[bowler]["bowling"] += K_FACTOR * (S_bowler - E_bowler)

    # Batch update ratings (separately for batting and bowling)
    bulk_updates = []
    for player, ratings in player_ratings.items():
        update_query = {"$set": {}}

        if "batting" in ratings:
            update_query["$push"] = {"batting_rating": {"date": match_date, "rating": ratings["batting"]}}
        if "bowling" in ratings:
            if "$push" in update_query:
                update_query["$push"]["bowling_rating"] = {"date": match_date, "rating": ratings["bowling"]}
            else:
                update_query["$push"] = {"bowling_rating": {"date": match_date, "rating": ratings["bowling"]}}

        bulk_updates.append(UpdateOne({"player_name": player}, update_query, upsert=True))

    if bulk_updates:
        player_ratings_collection.bulk_write(bulk_updates)
    
    return


def update_all_player_ratings():
    """Processes all match files, applying seasonal decay between seasons."""
    current_season = None  

    for file in tqdm(get_match_files(), desc="Processing Matches"):
        
        df = pd.read_csv(file, usecols=["season", "start_date"])
        season = df["season"].iloc[0]
        first_match_date_of_season = df["start_date"].iloc[0]

        if (current_season is None or season != current_season) and DECAY_RATE > 0:
            apply_seasonal_decay(first_match_date_of_season)
            current_season = season

        process_match_file(file)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update player Elo ratings.")
    parser.add_argument('--force-reprocess', action='store_true', help='Force reprocessing of all match files.')
    args = parser.parse_args()

    existing_processed_count = processed_matches_collection.count_documents({})
    existing_elo_data = player_ratings_collection.count_documents({})

    if existing_processed_count > 0 and existing_elo_data > 0:
        if args.force_reprocess:
            print("\nReprocessing all files...")
            player_ratings_collection.delete_many({})
            processed_matches_collection.delete_many({})
        else:
            print("\nProcessing only new match files...\n")

    else:
        print("\nProcessing all match files...\n")

    update_all_player_ratings()
    print("\nElo ratings updated for all players.")
