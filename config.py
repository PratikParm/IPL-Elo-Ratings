class Config:

    # Venue Factors
    BASE_BATTING_FACTORS = {0: 0.3, 1: 0.45, 2: 0.6, 3: 0.7, 4: 0.8, 5: 0.9, 6: 1.0, "wicket": 0.0}
    BASE_BOWLING_FACTORS = {0: 0.7, 1: 0.55, 2: 0.4, 3: 0.3, 4: 0.25, 5: 0.2, 6: 0.1, "wicket": 1.0, "wide": 0.2, "no-ball": 0.07}
    ADJUSTMENT_FACTOR = 0.1

    # Elo Ratings
    DEFAULT_ELO = 1200
    K_FACTOR = 10
    DECAY_TIME_THRESHOLD = 400  # Inactive for over a year
    DECAY_RATE = 30  # Elo points decay for inactive players