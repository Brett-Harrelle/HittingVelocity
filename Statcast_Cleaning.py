import pandas as pd
from sqlalchemy import text
from Database_Connection import engine
from Statcast_Data import all_data as statcast_df

print("Starting data cleaning with fixes...")

#Import Statcast data
print(f"Loaded {len(statcast_df):,} pitches from Statcast_Data")

#Only use fastballs
#FF = 4-seam
#FT = 2-seam
#SI = sinker
#FC = cutter
fastballs = ['FF', 'FT', 'SI', 'FC']
all_data = statcast_df[statcast_df['pitch_type'].isin(fastballs)].copy()
all_data = all_data[all_data['game_type'] == 'R']

#Statcast_Data.py
if 'SeasonYear' not in all_data.columns and 'game_date' in all_data.columns:
    all_data['game_date'] = pd.to_datetime(all_data['game_date'])
    all_data['SeasonYear'] = all_data['game_date'].dt.year

print(f"\nTotal fastball pitches after filtering: {len(all_data):,}")

#load reference tables
players_df = pd.read_sql("SELECT PlayerID, BirthDate FROM Players", engine)
teams_df = pd.read_sql("SELECT TeamID, TeamName FROM Teams", engine)

#Adjust team names
abbr_to_name = {
    'NYY': 'Yankees', 'BOS': 'Red Sox', 'TOR': 'Blue Jays', 'TB': 'Rays', 'BAL': 'Orioles',
    'CWS': 'White Sox', 'CLE': 'Indians', 'DET': 'Tigers', 'KC': 'Royals', 'MIN': 'Twins',
    'HOU': 'Astros', 'LAA': 'Angels', 'ATH': 'Athletics', 'SEA': 'Mariners', 'TEX': 'Rangers',
    'NYM': 'Mets', 'PHI': 'Phillies', 'ATL': 'Braves', 'MIA': 'Marlins', 'WSH': 'Nationals',
    'CHC': 'Cubs', 'STL': 'Cardinals', 'MIL': 'Brewers', 'CIN': 'Reds', 'PIT': 'Pirates',
    'LAD': 'Dodgers', 'SF': 'Giants', 'SD': 'Padres', 'AZ': 'Diamondbacks', 'COL': 'Rockies'
}

all_data['batter_team_abbr'] = all_data.apply(
    lambda r: r['home_team'] if r['inning_topbot'] == 'Bot' else r['away_team'],
    axis=1
)
all_data['TeamName'] = all_data['batter_team_abbr'].map(abbr_to_name)
all_data = all_data.merge(teams_df, on='TeamName', how='left')
all_data = all_data.dropna(subset=['TeamID'])
all_data['TeamID'] = all_data['TeamID'].astype(int)

#Clean existing tables
print("\nCleaning existing tables...")
with engine.begin() as conn:
    conn.execute(text("DELETE FROM PlayerPitches"))
    conn.execute(text("DELETE FROM PlayerSeasons"))
print("✅ Tables cleared")


#Create PlayerSeasons
player_seasons = all_data[['batter', 'TeamID', 'SeasonYear']].copy()
player_seasons.rename(columns={'batter': 'PlayerID'}, inplace=True)
player_seasons = player_seasons.groupby(['PlayerID', 'TeamID', 'SeasonYear']).size().reset_index(name='pitch_count')
player_seasons = player_seasons[['PlayerID', 'TeamID', 'SeasonYear']]

player_seasons = player_seasons.merge(players_df, on='PlayerID', how='inner')
player_seasons['BirthDate'] = pd.to_datetime(player_seasons['BirthDate'])
player_seasons['Age'] = player_seasons['SeasonYear'] - player_seasons['BirthDate'].dt.year
player_seasons = player_seasons[['PlayerID', 'TeamID', 'SeasonYear', 'Age']]
player_seasons = player_seasons.drop_duplicates(subset=['PlayerID', 'TeamID', 'SeasonYear'])


#Insert PlayerSeasons
chunk_size = 500
total_seasons = 0
for start in range(0, len(player_seasons), chunk_size):
    end = start + chunk_size
    chunk = player_seasons.iloc[start:end]

    with engine.begin() as conn:
        records = chunk.to_dict('records')
        result = conn.execute(
            text(
                "INSERT IGNORE INTO PlayerSeasons (PlayerID, TeamID, SeasonYear, Age) VALUES (:PlayerID, :TeamID, :SeasonYear, :Age)"),
            records
        )
        total_seasons += result.rowcount

print(f"✅ PlayerSeasons inserted: {total_seasons}")


#Load PlayerSeasons
player_seasons_db = pd.read_sql(
    "SELECT PlayerSeasonID, PlayerID, TeamID, SeasonYear FROM PlayerSeasons",
    engine
)


#Create PlayerPitches
print("\nCreating PlayerPitches with correct swing/miss logic...")

#Merge to get PlayerSeasonID
pitches_df = all_data.merge(
    player_seasons_db,
    left_on=['batter', 'TeamID', 'SeasonYear'],
    right_on=['PlayerID', 'TeamID', 'SeasonYear'],
    how='inner'
)

print(f"Pitches matched: {len(pitches_df):,}")

print("\nSample pitch descriptions:")
print(pitches_df['description'].value_counts().head(10))

#Swing and Miss Logic
swing_descriptions = [
    'swinging_strike', 'foul', 'hit_into_play',
    'swinging_strike_blocked', 'foul_tip', 'foul_bunt',
    'missed_bunt', 'bunt_foul_tip'
]

miss_descriptions = [
    'swinging_strike', 'swinging_strike_blocked',
    'missed_bunt', 'foul_tip'
]

pitches_df['Swing'] = pitches_df['description'].isin(swing_descriptions)
pitches_df['Miss'] = pitches_df['description'].isin(miss_descriptions)
pitches_df['BallInPlay'] = pitches_df['description'] == 'hit_into_play'


#Verify Swing and Miss logic
swing_count = pitches_df['Swing'].sum()
miss_count = pitches_df['Miss'].sum()
bip_count = pitches_df['BallInPlay'].sum()

print(f"\nSwing detection:")
print(f"  Total pitches: {len(pitches_df):,}")
print(f"  Swings: {swing_count:,} ({swing_count / len(pitches_df) * 100:.1f}%)")
print(f"  Misses: {miss_count:,} ({miss_count / swing_count * 100:.1f}% of swings)")
print(f"  Balls in play: {bip_count:,}")


#Hit Outcomes
pitches_df['IsSingle'] = (pitches_df['events'] == 'single').fillna(False)
pitches_df['IsDouble'] = (pitches_df['events'] == 'double').fillna(False)
pitches_df['IsTriple'] = (pitches_df['events'] == 'triple').fillna(False)
pitches_df['IsHomeRun'] = (pitches_df['events'] == 'home_run').fillna(False)
pitches_df['IsStrikeout'] = (pitches_df['events'] == 'strikeout').fillna(False)
pitches_df['IsWalk'] = (pitches_df['events'] == 'walk').fillna(False)

out_events = [
    'field_out', 'force_out', 'grounded_into_double_play',
    'fielders_choice', 'fielders_choice_out', 'sac_fly',
    'sac_bunt', 'sac_fly_double_play', 'sac_bunt_double_play'
]
pitches_df['IsOut'] = pitches_df['events'].isin(out_events).fillna(False)


#Quality Contact Metric
for col in ['barrel', 'squared_up', 'hard_hit']:
    if col in pitches_df.columns:
        pitches_df[col] = pitches_df[col].fillna(False)
    else:
        pitches_df[col] = False
        print(f"⚠️  Column '{col}' not found in data, setting to False")


#Create PlayerPitches
player_pitches_df = pitches_df[[
    'PlayerSeasonID',
    'pitch_type',
    'release_speed',
    'Swing',
    'Miss',
    'BallInPlay',
    'IsSingle',
    'IsDouble',
    'IsTriple',
    'IsHomeRun',
    'IsOut',
    'IsStrikeout',
    'IsWalk',
    'barrel',
    'squared_up',
    'hard_hit',
    'p_throws',
    'launch_speed',
    'launch_angle'
]].copy()

player_pitches_df.rename(columns={
    'pitch_type': 'PitchType',
    'release_speed': 'PitchSpeed',
    'barrel': 'Barrel',
    'squared_up': 'SquaredUp',
    'hard_hit': 'HardHit',
    'p_throws': 'PitcherThrows',
    'launch_speed': 'ExitVelocity',
    'launch_angle': 'LaunchAngle'
}, inplace=True)

bool_cols = ['Swing', 'Miss', 'BallInPlay', 'IsSingle', 'IsDouble', 'IsTriple',
             'IsHomeRun', 'IsOut', 'IsStrikeout', 'IsWalk', 'Barrel', 'SquaredUp', 'HardHit']
for col in bool_cols:
    player_pitches_df[col] = player_pitches_df[col].astype(bool)

print(f"\nPlayerPitches records: {len(player_pitches_df):,}")


#Insert into Database
chunk_size = 1000
total_pitches = 0

for start in range(0, len(player_pitches_df), chunk_size):
    end = start + chunk_size
    chunk = player_pitches_df.iloc[start:end]

    chunk.to_sql(
        'PlayerPitches',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    total_pitches += len(chunk)
    if start % 10000 == 0:
        print(f"  Inserted {total_pitches:,} pitches...")

print(f"\n✅ Total PlayerPitches inserted: {total_pitches:,}")


#Verification
print("\n" + "=" * 50)
print("VERIFICATION")
print("=" * 50)

with engine.begin() as conn:

    result = conn.execute(text("""
                               SELECT SUM(Swing)                               as total_swings,
                                      SUM(Miss)                                as total_misses,
                                      COUNT(*)                                 as total_pitches,
                                      ROUND(SUM(Swing) * 100.0 / COUNT(*), 2)  as swing_percentage,
                                      ROUND(SUM(Miss) * 100.0 / SUM(Swing), 2) as whiff_percentage
                               FROM PlayerPitches
                               """))

    stats = result.fetchone()
    print(f"\nSwing/Miss Statistics:")
    print(f"  Total Pitches: {stats[2]:,}")
    print(f"  Total Swings: {stats[0]:,}")
    print(f"  Total Misses: {stats[1]:,}")
    print(f"  Swing%: {stats[3]}%")
    print(f"  Whiff%: {stats[4]}%")


    result = conn.execute(text("""
                               SELECT MIN(PitchSpeed) as min_velo,
                                      MAX(PitchSpeed) as max_velo,
                                      AVG(PitchSpeed) as avg_velo
                               FROM PlayerPitches
                               """))

    velo_stats = result.fetchone()
    print(f"\nVelocity Statistics:")
    print(f"  Min: {velo_stats[0]:.1f} mph")
    print(f"  Max: {velo_stats[1]:.1f} mph")
    print(f"  Avg: {velo_stats[2]:.1f} mph")

print("\n🎯 Data loading complete!")
