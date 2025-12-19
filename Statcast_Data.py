from pybaseball import statcast
import pandas as pd


#Each season
seasons = [
    ('2021-03-01', '2021-11-01'),
    ('2022-03-01', '2022-11-01'),
    ('2023-03-01', '2023-11-01'),
    ('2024-03-01', '2024-11-01'),
    ('2025-03-01', '2025-11-01')
]

all_data = pd.DataFrame()

for start, end in seasons:
    print(f"Pulling Statcast data from {start} to {end}...")
    season_data = statcast(start_dt=start, end_dt=end)

    #Keep only regular season games
    season_data = season_data[season_data['game_type'] == 'R']

    all_data = pd.concat([all_data, season_data], ignore_index=True)

print(all_data.head())
print(all_data.columns)
