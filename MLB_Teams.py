import pandas as pd
from Database_Connection import engine



teams_data = [
    {'TeamName': 'Yankees', 'TeamCity': 'New York', 'TeamLeague': 'AL', 'Division': 'East'},
    {'TeamName': 'Red Sox', 'TeamCity': 'Boston', 'TeamLeague': 'AL', 'Division': 'East'},
    {'TeamName': 'Blue Jays', 'TeamCity': 'Toronto', 'TeamLeague': 'AL', 'Division': 'East'},
    {'TeamName': 'Rays', 'TeamCity': 'Tampa Bay', 'TeamLeague': 'AL', 'Division': 'East'},
    {'TeamName': 'Orioles', 'TeamCity': 'Baltimore', 'TeamLeague': 'AL', 'Division': 'East'},

    {'TeamName': 'White Sox', 'TeamCity': 'Chicago', 'TeamLeague': 'AL', 'Division': 'Central'},
    {'TeamName': 'Indians', 'TeamCity': 'Cleveland', 'TeamLeague': 'AL', 'Division': 'Central'},
    {'TeamName': 'Tigers', 'TeamCity': 'Detroit', 'TeamLeague': 'AL', 'Division': 'Central'},
    {'TeamName': 'Royals', 'TeamCity': 'Kansas City', 'TeamLeague': 'AL', 'Division': 'Central'},
    {'TeamName': 'Twins', 'TeamCity': 'Minnesota', 'TeamLeague': 'AL', 'Division': 'Central'},

    {'TeamName': 'Astros', 'TeamCity': 'Houston', 'TeamLeague': 'AL', 'Division': 'West'},
    {'TeamName': 'Angels', 'TeamCity': 'Los Angeles', 'TeamLeague': 'AL', 'Division': 'West'},
    {'TeamName': 'Athletics', 'TeamCity': 'Oakland', 'TeamLeague': 'AL', 'Division': 'West'},
    {'TeamName': 'Mariners', 'TeamCity': 'Seattle', 'TeamLeague': 'AL', 'Division': 'West'},
    {'TeamName': 'Rangers', 'TeamCity': 'Texas', 'TeamLeague': 'AL', 'Division': 'West'},

    {'TeamName': 'Mets', 'TeamCity': 'New York', 'TeamLeague': 'NL', 'Division': 'East'},
    {'TeamName': 'Phillies', 'TeamCity': 'Philadelphia', 'TeamLeague': 'NL', 'Division': 'East'},
    {'TeamName': 'Braves', 'TeamCity': 'Atlanta', 'TeamLeague': 'NL', 'Division': 'East'},
    {'TeamName': 'Marlins', 'TeamCity': 'Miami', 'TeamLeague': 'NL', 'Division': 'East'},
    {'TeamName': 'Nationals', 'TeamCity': 'Washington', 'TeamLeague': 'NL', 'Division': 'East'},

    {'TeamName': 'Cubs', 'TeamCity': 'Chicago', 'TeamLeague': 'NL', 'Division': 'Central'},
    {'TeamName': 'Cardinals', 'TeamCity': 'St. Louis', 'TeamLeague': 'NL', 'Division': 'Central'},
    {'TeamName': 'Brewers', 'TeamCity': 'Milwaukee', 'TeamLeague': 'NL', 'Division': 'Central'},
    {'TeamName': 'Reds', 'TeamCity': 'Cincinnati', 'TeamLeague': 'NL', 'Division': 'Central'},
    {'TeamName': 'Pirates', 'TeamCity': 'Pittsburgh', 'TeamLeague': 'NL', 'Division': 'Central'},

    {'TeamName': 'Dodgers', 'TeamCity': 'Los Angeles', 'TeamLeague': 'NL', 'Division': 'West'},
    {'TeamName': 'Giants', 'TeamCity': 'San Francisco', 'TeamLeague': 'NL', 'Division': 'West'},
    {'TeamName': 'Padres', 'TeamCity': 'San Diego', 'TeamLeague': 'NL', 'Division': 'West'},
    {'TeamName': 'Diamondbacks', 'TeamCity': 'Arizona', 'TeamLeague': 'NL', 'Division': 'West'},
    {'TeamName': 'Rockies', 'TeamCity': 'Colorado', 'TeamLeague': 'NL', 'Division': 'West'}
]

teams_df = pd.DataFrame(teams_data)
teams_df.to_sql('Teams', engine, if_exists='append', index=False)

print("Teams table populated successfully!")
