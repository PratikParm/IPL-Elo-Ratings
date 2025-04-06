# IPL ELO Ratings Based on ball-by-ball Outcomes

This is a project on a ELO based rating system for cricket players in the T20 format. The project uses the ball-by-ball data for Indian Premier League and based on the outcome of a ball, updates the batting and bowling elo rating of the players involved.

## Usage

The project requires `python==3.10.16`. Get started by running the following command:

```bash
git clone https://github.com/PratikParm/IPL-Elo-Ratings.git
cd IPL-Elo-Ratings
pip install -r requirements.txt
```

First calculate the venue based elo factors by running the command:
```bash
python scripts/venue_factors.py
```
This will create a MongoDB collection and save the elo factors for all the venues.

Next, calculate the players elo by running the command:
```bash
python scripts/calculate_elo.py
```
This will create a MongoDB collection with all the players and their elo ratings at the end of all matches they have played.

## Dataset
The dataset used in the project is the cricksheet Indian Premier League Matches data.
Source: https://cricsheet.org/downloads/ipl_male_csv2.zip
