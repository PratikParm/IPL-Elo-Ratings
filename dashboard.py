import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go 
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
from pymongo import MongoClient

# Load environment variables
load_dotenv()
client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["cricket_elo"]
player_ratings_collection = db["player_ratings"]

# Fetch unique seasons
seasons = player_ratings_collection.distinct("batting_rating.date")  # Get all dates
seasons = sorted(set(pd.to_datetime(seasons).year))  # Extract unique years
seasons.insert(0, "All")  # Add "All" option

# Initialize Dash App
app = dash.Dash(__name__)
server = app.server  # Needed for deployment

# Layout
app.layout = html.Div(className="dashboard-container", children=[

    # Dashboard Title
    html.Header(children=[
        html.H2("IPL Player Elo Rating Dashboard", className="dashboard-title")
    ]),

    html.H1("Current Leaderboards", className="section-title"),

    # Season Dropdown
    html.Div(className="dropdown-wrapper", children=[
        html.Label("Select Season", className="dropdown-label"),
        dcc.Dropdown(
            id="season-selector",
            options=[{"label": str(year), "value": year} for year in seasons],
            value="All",
            placeholder="Select Season",
            className="dropdown"
        )
    ]),

    # Leaderboard
    html.Div(className="leaderboard-container", children=[
        
        html.Div(className="leaderboard-box", children=[
            html.H2("🏏 Top 10 Batters", className="section-title"),
            html.Ul(id="top-batters-leaderboard", className="leaderboard"),
        ]),
        html.Div(className="leaderboard-box", children=[
            html.H2("🎯 Top 10 Bowlers", className="section-title"),
            html.Ul(id="top-bowlers-leaderboard", className="leaderboard"),
        ])
    ]),

    # Player Elo Evolution Section
    html.H1("📈 Player Elo Evolution", className="section-title"),

    html.Div(className="dropdown-container", children=[
        dcc.Dropdown(id="player-selector", placeholder="Select a Player", className="dropdown"),
    ]),

    html.Div(className="radio-container", children=[
        dcc.RadioItems(
            id="rating-toggle",
            options=[{"label": "Batting", "value": "batting_rating"},
                     {"label": "Bowling", "value": "bowling_rating"}],
            value="batting_rating",
            labelStyle={"display": "inline-block", "margin-right": "10px"},
            className="RadioItems"
        )
    ]),

    html.Div(className="graph-container", children=[
        dcc.Graph(id="player-elo-graph")
    ]),

    html.H1("Peak Leaderboards", className="section-title"),

    # Manual Refresh Button
    html.Button("Refresh Leaderboard", id="refresh-button", n_clicks=0, style={"marginBottom": "20px"}, className="refresh-button"),

    # Peak Rating Leaderboards
    html.Div(className="leaderboard-container", children=[
    html.Div(className="leaderboard-box", children=[
        html.H2("🏏 Top 10 Peak Batting Ratings", className="section-title"),
        html.Ul(id="top-peak-batters-leaderboard", className="leaderboard"),
    ]),
    html.Div(className="leaderboard-box", children=[
        html.H2("🎯 Top 10 Peak Bowling Ratings", className="section-title"),
        html.Ul(id="top-peak-bowlers-leaderboard", className="leaderboard"),
    ])
    ]),
])

# Callback to update top 10 players
@app.callback(
    [Output("top-batters-leaderboard", "children"), 
     Output("top-bowlers-leaderboard", "children")],
    [Input("season-selector", "value")]
)
def update_top_players(season):
    """Fetch and display top 10 players for the selected season."""
    batter_data, bowler_data = [], []

    players = list(player_ratings_collection.find({}, {"_id": 0, "player_name": 1, "batting_rating": 1, "bowling_rating": 1}))

    for player in players:
        # Batting Ratings
        batting_ratings = player.get("batting_rating", [])
        if season != "All":
            batting_ratings = [r for r in batting_ratings if r["date"].startswith(str(season))]
        if batting_ratings:
            batter_data.append({"player_name": player["player_name"], "rating": int(batting_ratings[-1]["rating"])})

        # Bowling Ratings
        bowling_ratings = player.get("bowling_rating", [])
        if season != "All":
            bowling_ratings = [r for r in bowling_ratings if r["date"].startswith(str(season))]
        if bowling_ratings:
            bowler_data.append({"player_name": player["player_name"], "rating": int(bowling_ratings[-1]["rating"])})

    batter_data = sorted(batter_data, key=lambda x: x["rating"], reverse=True)[:10]
    bowler_data = sorted(bowler_data, key=lambda x: x["rating"], reverse=True)[:10]

    # Generate leaderboard items
    def generate_leaderboard(data):
        return [
            html.Li([
                html.Span(f"{idx + 1}. {player['player_name']}", className="leaderboard-player"),
                html.Span(str(player["rating"]), className="leaderboard-score")
            ], className=f"leaderboard-item {'gold' if idx == 0 else 'silver' if idx == 1 else 'bronze' if idx == 2 else ''}")
            for idx, player in enumerate(data)
        ]

    return generate_leaderboard(batter_data), generate_leaderboard(bowler_data)

# Callback to update player-specific rating evolution
@app.callback(
    [Output("player-selector", "options"), Output("player-elo-graph", "figure")],
    [Input("player-selector", "value"), Input("rating-toggle", "value")],
    prevent_initial_call=False  # Ensures callback fires on first load
)
def update_player_stats(selected_player, rating_type):
    """Update player-specific rating evolution graph based on the toggle selection."""
    # Fetch distinct players for dropdown
    players = player_ratings_collection.distinct("player_name")
    player_options = [{"label": p, "value": p} for p in players]

    if not selected_player:
        fig = go.Figure(layout_title_text="Select a Player to View Elo Progress")
        # Update figure layout with IPL styling
        fig.update_layout(
            title=dict(
                text="Select a Player to View Elo Progress",
                font=dict(size=22, color="#ffd700"),  # Gold Title
                x=0.5  # Center Align
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
            plot_bgcolor="#161b22",  # Dark Background
            paper_bgcolor="#0d1117",  # Dashboard Background
            margin=dict(l=60, r=20, t=50, b=50),
            legend=dict(
                font=dict(color="#c9d1d9"),
                bgcolor="rgba(26,27,30,0.6)",
                bordercolor="#30363d",
                borderwidth=1
            )
        )
        return player_options, fig
        return player_options, fig

    # Fetch player data from MongoDB
    player_data = player_ratings_collection.find_one(
        {"player_name": selected_player},
        {"_id": 0, "batting_rating": 1, "bowling_rating": 1}
    )

    df = []
    if player_data and rating_type in player_data and isinstance(player_data[rating_type], list):
        for entry in player_data[rating_type]:
            df.append({"date": entry["date"], "rating": entry["rating"]})

    df = pd.DataFrame(df)

    # Handle case where no data is available for the selected rating type
    if df.empty:
        return player_options, go.Figure(layout_title_text=f"{selected_player} has no {rating_type.replace('_', ' ')} data.")

    # Convert date to datetime and sort
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
                line=dict(color="#6a0dad", width=3),  # IPL Purple Line
                marker=dict(size=8, color="#1c1b9b", line=dict(width=1, color="#ffd700"))  # IPL Blue Markers with Gold Outline
            )
        ]
    )

    # Update figure layout with IPL styling
    fig.update_layout(
        title=dict(
            text=f"{selected_player} {rating_type.replace('_', ' ').title()} Progress",
            font=dict(size=22, color="#ffd700"),  # Gold Title
            x=0.5  # Center Align
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
        plot_bgcolor="#161b22",  # Dark Background
        paper_bgcolor="#0d1117",  # Dashboard Background
        margin=dict(l=60, r=20, t=50, b=50),
        legend=dict(
            font=dict(color="#c9d1d9"),
            bgcolor="rgba(26,27,30,0.6)",
            bordercolor="#30363d",
            borderwidth=1
        )
    )
    return player_options, fig

@app.callback(
    [Output("top-peak-batters-leaderboard", "children"), 
     Output("top-peak-bowlers-leaderboard", "children")],
    [Input("refresh-button", "n_clicks")]
)
def update_peak_rating_leaderboards(_):
    """Fetch and display top 10 peak ratings along with the year achieved."""
    peak_batter_data, peak_bowler_data = [], []

    # Fetch all players
    players = list(player_ratings_collection.find({}, {"_id": 0, "player_name": 1, "batting_rating": 1, "bowling_rating": 1}))

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

    # Sort by highest rating and keep top 10
    peak_batter_data = sorted(peak_batter_data, key=lambda x: x["rating"], reverse=True)[:10]
    peak_bowler_data = sorted(peak_bowler_data, key=lambda x: x["rating"], reverse=True)[:10]

    # Generate leaderboard items
    def generate_peak_leaderboard(data):
        return [
            html.Li([
                html.Span(f"{idx + 1}. {player['player_name']} ({player['year']})", className="leaderboard-player"),
                html.Span(str(player["rating"]), className="leaderboard-score")
            ], className=f"leaderboard-item {'gold' if idx == 0 else 'silver' if idx == 1 else 'bronze' if idx == 2 else ''}")
            for idx, player in enumerate(data)
        ]

    return generate_peak_leaderboard(peak_batter_data), generate_peak_leaderboard(peak_bowler_data)

# Run app
if __name__ == "__main__":
    app.run(debug=True)
