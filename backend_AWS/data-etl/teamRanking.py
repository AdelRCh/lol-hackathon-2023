import pandas as pd
import numpy as np

df= pd.read_csv('data_2023_sorted_latest_rating.csv')
df['leagueId'] = df['leagueId'].astype(np.int64)
df['team_id'] = df['team_id'].astype(np.int64)


# Sort the DataFrame by 'league_id', 'rating', and 'team_id' to ensure proper ranking
df.sort_values(by=['leagueId', 'rating', 'team_id'], ascending=[True, False, True], inplace=True)

# Reset the index of the DataFrame after sorting
df.reset_index(drop=True, inplace=True)

# Use the rank method to create the 'rank_team' column
df['rank_team'] = df.groupby('leagueId').cumcount() + 1

# Display the resulting DataFrame
df.head()
df.to_csv("teamRankings.csv",index=None)
