import streamlit as st

# ---- set_page_config MUST be first Streamlit command ----
st.set_page_config(page_title="IPL Player Elo Rating Dashboard", layout="wide")

import os
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient

# ---- Inject custom CSS for IPL color scheme ----
st.markdown("""
    <style>
    body {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    .dashboard-title {
        color: #ffd700;
        text-align: center;
        font-size: 2.5rem;
        margin-bottom: 1rem;
    }
    .section-title {
        color: #ffd700;
        margin-top: 2rem;
        margin-bottom: 1rem;
        font-size: 2.1rem !important; /* Increased font size */
        text-align: center;
    }
    .stButton>button {
        background-color: #161b22;
        color: #ffd700;
        border: 1px solid #ffd700;
        border-radius: 8px;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #ffd700;
        color: #161b22;
    }
    .stSelectbox, .stRadio, .stTable, .stDataFrame {
        background-color: #161b22 !important;
        color: #c9d1d9 !important;
        border-radius: 8px;
    }
    .css-1v0mbdj, .css-1d391kg {  /* leaderboard table */
        background-color: #161b22 !important;
        color: #c9d1d9 !important;
    }
    .gold {
        color: #ffd700 !important;
        font-weight: bold;
    }
    .silver {
        color: #c0c0c0 !important;
        font-weight: bold;
    }
    .bronze {
        color: #cd7f32 !important;
        font-weight: bold;
    }
    /* Reduce dropdown width */
    .stSelectbox, .stSelectbox > div {
        max-width: 300px !important;
        min-width: 180px !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }
    /* Reduce radio width */
    .stRadio {
        max-width: 300px !important;
        min-width: 180px !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }
    </style>
""", unsafe_allow_html=True)

# Load environment variables
load_dotenv()
client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["cricket_elo"]
player_ratings_collection = db["player_ratings"]

# Fetch unique seasons
seasons = player_ratings_collection.distinct("batting_rating.date")
seasons = sorted(set(pd.to_datetime(seasons).year))
seasons.insert(0, "All")

# Dashboard Title
st.markdown(
    "<div class='dashboard-title'>IPL Player Elo Rating Dashboard</div>",
    unsafe_allow_html=True,
)

st.markdown("<div class='section-title'>Current Leaderboards</div>", unsafe_allow_html=True)

# Season Dropdown
season = st.selectbox("Select Season", seasons, index=0)

# Fetch and display top 10 players for the selected season
players = list(player_ratings_collection.find({}, {"_id": 0, "player_name": 1, "batting_rating": 1, "bowling_rating": 1}))

# --- Prepare data with elo gain for leaderboard ---
batter_data, bowler_data = [], []

for player in players:
    # Batting Ratings
    batting_ratings = player.get("batting_rating", [])
    if season != "All":
        batting_ratings = [r for r in batting_ratings if r["date"].startswith(str(season))]
    if batting_ratings:
        last = int(batting_ratings[-1]["rating"])
        prev = int(batting_ratings[-2]["rating"]) if len(batting_ratings) > 1 else None
        elo_gain = last - prev if prev is not None else None
        batter_data.append({"player_name": player["player_name"], "rating": last, "elo_gain": elo_gain})

    # Bowling Ratings
    bowling_ratings = player.get("bowling_rating", [])
    if season != "All":
        bowling_ratings = [r for r in bowling_ratings if r["date"].startswith(str(season))]
    if bowling_ratings:
        last = int(bowling_ratings[-1]["rating"])
        prev = int(bowling_ratings[-2]["rating"]) if len(bowling_ratings) > 1 else None
        elo_gain = last - prev if prev is not None else None
        bowler_data.append({"player_name": player["player_name"], "rating": last, "elo_gain": elo_gain})

batter_data = sorted(batter_data, key=lambda x: x["rating"], reverse=True)[:10]
bowler_data = sorted(bowler_data, key=lambda x: x["rating"], reverse=True)[:10]

def leaderboard_table(data):
    df = pd.DataFrame(data)
    df.index = df.index + 1
    if not df.empty:
        # Add Elo Gain column with arrows and color
        def format_gain(row):
            if pd.isna(row["elo_gain"]):
                return ""
            color = "#27ae60" if row["elo_gain"] > 0 else "#e74c3c"
            sign = "+" if row["elo_gain"] > 0 else ""
            return f"<span style='color:{color}; font-weight:bold;'>{sign}{row['elo_gain']}</span>"

        df["Elo Gain"] = df.apply(format_gain, axis=1)
        df = df.rename(columns={"player_name": "Player", "rating": "Rating"})
        # Show Player and Elo Gain in one column
        df["Player"] = df.apply(
            lambda row: f"{row['Player']} {row['Elo Gain']}", axis=1
        )
        return df[["Player", "Rating"]].to_html(escape=False, index=True)
    else:
        return "<div>No data available.</div>"

# Place the two tables side by side and centrally aligned, with titles above each table
st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: flex-start; gap: 40px; margin-bottom: 32px;">
        <div style="min-width: 320px; max-width: 400px;">
            <div class='section-title' style='text-align:center'>🏏 Top 10 Batters</div>
            {leaderboard_table(batter_data)}
        </div>
        <div style="min-width: 320px; max-width: 400px;">
            <div class='section-title' style='text-align:center'>🎯 Top 10 Bowlers</div>
            {leaderboard_table(bowler_data)}
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown("<div class='section-title'>📈 Player Elo Evolution</div>", unsafe_allow_html=True)

# Player selector
player_names = player_ratings_collection.distinct("player_name")
selected_player = st.selectbox("Select a Player", [""] + sorted(player_names))

rating_type = st.radio("Select Rating Type", ["batting_rating", "bowling_rating"], format_func=lambda x: "Batting" if x == "batting_rating" else "Bowling")

if selected_player:
    player_data = player_ratings_collection.find_one(
        {"player_name": selected_player},
        {"_id": 0, "batting_rating": 1, "bowling_rating": 1}
    )
    df = []
    if player_data and rating_type in player_data and isinstance(player_data[rating_type], list):
        for entry in player_data[rating_type]:
            df.append({"date": entry["date"], "rating": entry["rating"]})
    df = pd.DataFrame(df)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", inplace=True)
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=df.index,
                    y=df["rating"].tolist(),
                    mode="lines+markers",
                    name=rating_type.replace("_", " ").title(),
                    line=dict(color="#6a0dad", width=3),
                    marker=dict(size=8, color="#1c1b9b", line=dict(width=1, color="#ffd700"))
                )
            ]
        )
        fig.update_layout(
            title=dict(
                text=f"{selected_player} {rating_type.replace('_', ' ').title()} Progress",
                font=dict(size=22, color="#ffd700"),
                x=0.5
            ),
            xaxis=dict(
                title=dict(text="Innings", font=dict(color="#c9d1d9")),
                tickfont=dict(color="#c9d1d9"),
                gridcolor="#30363d"
            ),
            yaxis=dict(
                title=dict(text="Rating", font=dict(color="#c9d1d9")),
                tickfont=dict(color="#c9d1d9"),
                gridcolor="#30363d"
            ),
            plot_bgcolor="#161b22",
            paper_bgcolor="#0d1117",
            margin=dict(l=60, r=20, t=50, b=50),
            legend=dict(
                font=dict(color="#c9d1d9"),
                bgcolor="rgba(26,27,30,0.6)",
                bordercolor="#30363d",
                borderwidth=1
            )
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"{selected_player} has no {rating_type.replace('_', ' ')} data.")
else:
    st.info("Select a Player to View Elo Progress")

st.markdown("<div class='section-title'>Peak Leaderboards</div>", unsafe_allow_html=True)

if st.button("Refresh Leaderboard"):
    st.experimental_rerun()

# Peak Rating Leaderboards
peak_batter_data, peak_bowler_data = [], []

for player in players:
    # Find Peak Batting Rating
    if "batting_rating" in player and isinstance(player["batting_rating"], list):
        peak_batting = max(player["batting_rating"], key=lambda r: r["rating"], default=None)
        if peak_batting:
            peak_batter_data.append({
                "player_name": player["player_name"],
                "rating": int(peak_batting["rating"]),
                "year": pd.to_datetime(peak_batting["date"]).year
            })

    # Find Peak Bowling Rating
    if "bowling_rating" in player and isinstance(player["bowling_rating"], list):
        peak_bowling = max(player["bowling_rating"], key=lambda r: r["rating"], default=None)
        if peak_bowling:
            peak_bowler_data.append({
                "player_name": player["player_name"],
                "rating": int(peak_bowling["rating"]),
                "year": pd.to_datetime(peak_bowling["date"]).year
            })

peak_batter_data = sorted(peak_batter_data, key=lambda x: x["rating"], reverse=True)[:10]
peak_bowler_data = sorted(peak_bowler_data, key=lambda x: x["rating"], reverse=True)[:10]

def peak_leaderboard_table(data, title, emoji):
    df = pd.DataFrame(data)
    df.index = df.index + 1
    st.markdown(f"<div class='section-title'>{emoji} {title}</div>", unsafe_allow_html=True)
    if not df.empty:
        df = df.rename(columns={"player_name": "Player", "rating": "Peak Rating", "year": "Year"})
        st.table(df)
    else:
        st.write("No data available.")

col3, col4 = st.columns(2)
with col3:
    peak_leaderboard_table(peak_batter_data, "Top 10 Peak Batting Ratings", "🏏")
with col4:
    peak_leaderboard_table(peak_bowler_data, "Top 10 Peak Bowling Ratings", "🎯")