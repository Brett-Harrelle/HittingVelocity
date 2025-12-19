import time
import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from Database_Connection import engine
from Statcast_Data import all_data as statcast_df

#Identify hitters
hitters_df = statcast_df[
    statcast_df['events'].notna() |
    statcast_df['description'].str.contains('hit_into_play', na=False) |
    statcast_df['description'].str.contains('swing', na=False)
]

#Get MLBAM ID
hitters_df = hitters_df[['batter']].drop_duplicates()
hitters_df = hitters_df.rename(columns={'batter': 'PlayerID'})

print(f"Unique hitters found: {len(hitters_df)}")

existing_players = pd.read_sql(
    "SELECT PlayerID FROM Players",
    engine
)

hitters_df = hitters_df[
    ~hitters_df['PlayerID'].isin(existing_players['PlayerID'])
]

print(f"New hitters to fetch from MLB API: {len(hitters_df)}")

#MLB STATS API
def fetch_player_from_mlb(player_id):
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get('people'):
            return None

        p = data['people'][0]

        return {
            "PlayerID": player_id,
            "FirstName": p.get('firstName', ''),
            "LastName": p.get('lastName', ''),
            "BirthDate": p.get('birthDate'),
            "Bats": p.get('batSide', {}).get('code', 'U'),
            "Throws": p.get('pitchHand', {}).get('code', 'U')
        }

    except Exception as e:
        print(f"API error for {player_id}: {e}")
        return None


players_to_insert = []

for i, pid in enumerate(hitters_df['PlayerID'], start=1):
    player = fetch_player_from_mlb(pid)
    if player:
        players_to_insert.append(player)

    time.sleep(0.15)

    if i % 100 == 0:
        print(f"Fetched {i} / {len(hitters_df)} hitters")

print(f"Players ready for insert: {len(players_to_insert)}")

#Insert into database
insert_sql = text("""
    INSERT INTO Players (
        PlayerID, FirstName, LastName, BirthDate, Bats, Throws
    )
    VALUES (
        :PlayerID, :FirstName, :LastName, :BirthDate, :Bats, :Throws
    )
""")

with engine.begin() as conn:
    for record in players_to_insert:
        try:
            conn.execute(insert_sql, record)
        except IntegrityError:
            continue

print("✅ Hitters successfully inserted into Players table")
