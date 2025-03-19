import os
from tqdm import tqdm
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from pymongo import MongoClient
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
player_ratings_collection.delete_many({})  # Reset player ratings (optional)

# Elo Configuration
DEFAULT_ELO = 1000
K_FACTOR = 32
DECAY_TIME_THRESHOLD = 400  # Inactive for over a year
DECAY_RATE = 30  # Elo points decay for inactive players


def get_match_files():
    """Returns a list of all match file paths in the directory."""
    return [MATCH_DATA_DIR / f for f in os.listdir(MATCH_DATA_DIR.resolve()) if not f.endswith("info.csv")]


def get_venue_factors(venue_name):
    """Fetches venue-specific outcome factors from MongoDB."""
    venue_doc = venue_factors_collection.find_one({"venue_name": venue_name})
    if venue_doc:
        return venue_doc["batting_factors"], venue_doc["bowling_factors"]
    else:
        print(f"⚠️ Warning: No venue factors found for {venue_name}, using defaults.")
        return None, None


def get_latest_player_rating(player_name):
    """Fetches the latest Elo rating of a player, or initializes it if missing."""
    player_doc = player_ratings_collection.find_one({"player_name": player_name})
    if player_doc and player_doc.get("ratings"):
        latest_rating = player_doc["ratings"][-1]
        return latest_rating["batting_rating"], latest_rating["bowling_rating"]
    return DEFAULT_ELO, DEFAULT_ELO


def expected_outcome(player_rating, opponent_rating):
    """Calculates the expected outcome probability using the Elo formula."""
    return 1 / (1 + 10 ** ((opponent_rating - player_rating) / 400))


def update_player_ratings(player_name, match_date, new_batting_rating, new_bowling_rating):
    """Updates player ratings in MongoDB, keeping a historical record."""
    player_ratings_collection.update_one(
        {"player_name": player_name},
        {"$push": {"ratings": {
            "date": match_date,
            "batting_rating": new_batting_rating,
            "bowling_rating": new_bowling_rating
        }}},
        upsert=True
    )


def apply_seasonal_decay(first_match_date_of_season):
    """Applies Elo decay for players who were inactive for over a year before a new season starts."""
    season_start_dt = datetime.strptime(first_match_date_of_season, "%Y-%m-%d")
    one_year_ago = season_start_dt - timedelta(days=DECAY_TIME_THRESHOLD)

    # Query all players
    all_players = player_ratings_collection.find({}, {"player_name": 1, "ratings": 1})

    for player in all_players:
        player_name = player["player_name"]
        if not player.get("ratings"):
            continue  # Skip players with no rating history

        last_rating_entry = player["ratings"][-1]
        last_played_date = datetime.strptime(last_rating_entry["date"], "%Y-%m-%d")

        if last_played_date < one_year_ago:

            decay_factor = (one_year_ago - last_played_date).days / DECAY_TIME_THRESHOLD
            if decay_factor < 1:
                decay_factor = 0  # Skip decay if player was active within the decay threshold
            
            # Player is inactive for over a year, apply decay
            new_batting_rating = last_rating_entry["batting_rating"] - decay_factor * DECAY_RATE
            new_bowling_rating = last_rating_entry["bowling_rating"] - decay_factor * DECAY_RATE

            # Update decayed ratings in DB
            player_ratings_collection.update_one(
                {"player_name": player_name},
                {"$push": {
                    "ratings": {
                        "date": first_match_date_of_season,
                        "batting_rating": new_batting_rating,
                        "bowling_rating": new_bowling_rating
                    }
                }}
            )


def process_match_file(file_path):
    """Processes a match file and updates player Elo ratings."""
    df = pd.read_csv(file_path)
    venue_name = df["venue"].iloc[0]
    match_date = df["start_date"].iloc[0]  # Using match start date

    # Fetch venue-specific factors
    batting_factors, bowling_factors = get_venue_factors(venue_name)
    if batting_factors is None or bowling_factors is None:
        return

    # Temporary dict to track ratings within a match
    player_ratings = {}

    for _, row in df.iterrows():
        if row['wicket_type'] == 'run out':
            continue  # Skip run-out as it doesn't affect Elo

        batsman = row["striker"]
        bowler = row["bowler"]
        runs = row["runs_off_bat"]

        # Determine outcome type
        if pd.notna(row["wicket_type"]):
            outcome = "wicket"
        elif row["wides"] > 0:
            outcome = "wide"
        elif row["noballs"] > 0:
            outcome = "no-ball"
        else:
            outcome = str(runs)  # Convert to string for MongoDB compatibility

        # Get or initialize ratings
        if batsman not in player_ratings:
            batting_rating, _ = get_latest_player_rating(batsman)
            player_ratings[batsman] = batting_rating

        if bowler not in player_ratings:
            _, bowling_rating = get_latest_player_rating(bowler)
            player_ratings[bowler] = bowling_rating

        # Compute expected values
        E_batsman = expected_outcome(player_ratings[batsman], player_ratings[bowler])
        E_bowler = expected_outcome(player_ratings[bowler], player_ratings[batsman])

        # Get actual outcome factor (S) from venue factors
        S_batsman = batting_factors.get(outcome, 0.5)  # Default to neutral
        S_bowler = bowling_factors.get(outcome, 0.5)

        # Elo update
        player_ratings[batsman] += K_FACTOR * (S_batsman - E_batsman)
        player_ratings[bowler] += K_FACTOR * (S_bowler - E_bowler)

    # Store updated ratings at end of match
    for player, rating in player_ratings.items():
        batting_rating, bowling_rating = get_latest_player_rating(player)

        # Determine if player was primarily a batsman or bowler in this match
        if player in df["striker"].values:
            batting_rating = rating
        if player in df["bowler"].values:
            bowling_rating = rating

        update_player_ratings(player, match_date, batting_rating, bowling_rating)


def update_all_player_ratings():
    """Processes all match files, applying seasonal decay between seasons."""
    current_season = None  # Track the current season

    for file in tqdm(get_match_files(), desc="Processing Matches"):
        try:
            df = pd.read_csv(file, usecols=["season", "start_date"])
            season = df["season"].iloc[0]  # Extract season
            first_match_date_of_season = df["start_date"].iloc[0]  # First match date

            # If new season detected, apply decay
            if current_season is None:
                current_season = season  # Initialize first season

            elif season != current_season:
                apply_seasonal_decay(first_match_date_of_season)  # Apply decay
                current_season = season  # Update tracked season

            process_match_file(file)  # Process match normally

        except Exception as e:
            print(f"⚠️ Error processing {file}: {e}")


if __name__ == '__main__':
    update_all_player_ratings()
    print("✅ Elo ratings updated for all players at the end of matches.")
