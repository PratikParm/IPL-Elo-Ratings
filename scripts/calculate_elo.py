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
K_FACTOR = 10
DECAY_TIME_THRESHOLD = 400  # Inactive for over a year
DECAY_RATE = 0  # Elo points decay for inactive players


def get_match_files():
    """Returns a list of all match file paths in the directory."""
    return sorted(
        [MATCH_DATA_DIR / f for f in os.listdir(MATCH_DATA_DIR.resolve()) if not f.endswith("info.csv")],
        key=lambda x: int(x.name.split(".")[0]))


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
    if player_doc:
        batting_ratings = player_doc.get("batting_rating", [])
        bowling_ratings = player_doc.get("bowling_rating", [])

        last_batting_rating = batting_ratings[-1] if batting_ratings else {"date": None, "batting_rating": DEFAULT_ELO}
        last_bowling_rating = bowling_ratings[-1] if bowling_ratings else {"date": None, "bowling_rating": DEFAULT_ELO}

        return last_batting_rating, last_bowling_rating

    return {"date": None, "batting_rating": DEFAULT_ELO}, {"date": None, "bowling_rating": DEFAULT_ELO}


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
                "$push": {"batting_rating": {"date": match_date, "batting_rating": batting_rating}}
            },
            upsert=True
        )

    elif bowling_rating is not None:
        player_ratings_collection.update_one(
            {"player_name": player_name},
            {
                "$push": {"bowling_rating": {"date": match_date, "bowling_rating": bowling_rating}}
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

        last_batting = batting_ratings[-1] if batting_ratings else {"date": None, "batting_rating": DEFAULT_ELO}
        last_bowling = bowling_ratings[-1] if bowling_ratings else {"date": None, "bowling_rating": DEFAULT_ELO}     
        

        if last_batting.get("date") is not None:
            last_batted_date = datetime.strptime(last_batting["date"], "%Y-%m-%d")

            if last_batted_date < one_year_ago:
                decay_factor = (one_year_ago - last_batted_date).days / DECAY_TIME_THRESHOLD
                if decay_factor < 1:
                    decay_factor = 0  # Skip decay if player was active within the threshold

                # Apply decay
                new_batting_rating = last_batting["batting_rating"] - decay_factor * DECAY_RATE

                # Update decayed ratings in DB
                update_player_ratings(player_name, first_match_date_of_season, batting_rating=new_batting_rating, bowling_rating=None)

        if last_bowling.get("date") is not None:
            last_bowled_date = datetime.strptime(last_bowling["date"], "%Y-%m-%d")

            if last_bowled_date < one_year_ago:
                decay_factor = (one_year_ago - last_bowled_date).days / DECAY_TIME_THRESHOLD
                if decay_factor < 1:
                    decay_factor = 0  # Skip decay if player was active within the threshold

                # Apply decay
                new_bowling_rating = last_bowling["bowling_rating"] - decay_factor * DECAY_RATE

                # Update decayed ratings in DB
                update_player_ratings(player_name, first_match_date_of_season, batting_rating=None, bowling_rating=new_bowling_rating)


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
    batting_ratings = {}
    bowling_ratings = {}

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

        # Get or initialize batting and bowling ratings
        if batsman not in batting_ratings:
            batting_rating, _ = get_latest_player_rating(batsman)
            batting_ratings[batsman] = batting_rating["batting_rating"]

        if bowler not in bowling_ratings:
            _, bowling_rating = get_latest_player_rating(bowler)
            bowling_ratings[bowler] = bowling_rating["bowling_rating"]

        # Compute expected values
        E_batsman = expected_outcome(batting_ratings[batsman], bowling_ratings[bowler])
        E_bowler = expected_outcome(bowling_ratings[bowler], batting_ratings[batsman])

        # Get actual outcome factor (S) from venue factors
        S_batsman = batting_factors.get(outcome, 0.5)  # Default to neutral
        S_bowler = bowling_factors.get(outcome, 0.5)

        # Elo update
        batting_ratings[batsman] += K_FACTOR * (S_batsman - E_batsman)
        bowling_ratings[bowler] += K_FACTOR * (S_bowler - E_bowler)

    # Store updated ratings at end of match
    for batsman, rating in batting_ratings.items():
        update_player_ratings(batsman, match_date, batting_rating=rating, bowling_rating=None)

    for bowler, rating in bowling_ratings.items():
        update_player_ratings(bowler, match_date, batting_rating=None, bowling_rating=rating)


def update_all_player_ratings():
    """Processes all match files, applying seasonal decay between seasons."""
    current_season = None  

    for file in tqdm(get_match_files(), desc="Processing Matches"):
        
        df = pd.read_csv(file, usecols=["season", "start_date"])
        season = df["season"].iloc[0]
        first_match_date_of_season = df["start_date"].iloc[0]

        if current_season is None or season != current_season:
            apply_seasonal_decay(first_match_date_of_season)
            current_season = season

        process_match_file(file)



if __name__ == '__main__':
    update_all_player_ratings()
    print("Elo ratings updated for all players.")
