import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

def process_and_score_games(filename='hackathon-riot-data.csv'):
    #Initialize the DataFrame
    df = pd.read_csv(filename, sep=';')
    # dataframe for potentially relevant features
    features_df = pd.DataFrame()

    #Cleaning up values that ended up overkill for the Hackathon's purposes
    df.drop(columns=[#'esportsPlatformId',
                 'NbWardsPlacedBlue',
                 'NbWardsPlacedRed',
                 'NbControlWardsPlacedBlue',
                 'NbControlWardsPlacedRed',
                 'NbWardsKilledBlue',
                 'NbWardsKilledRed',
                 'NbControlWardsKilledBlue',
                 'NbControlWardsKilledRed',
                 'NbCampsSecuredBlue',
                 'NbCampsSecuredRed',
                 'NbScuttlesBlue',
                 'NbScuttlesRed',
                 'NbDragonsBlue',
                 'NbDragonsRed',
                 'NbBaronsBlue',
                 'NbBaronsRed',
                 'NbEldersBlue',
                 'NbEldersRed',
                 'DragonSoulTimer',
                 'DragonSoulType',
                 'DragonSoulTaker',
                 'DragonSoulTeam'], inplace=True)


    # can't use a row without the winner label since that's what is being predicted
    missing_label_row = (df['winner'].isna() == True)
    df.drop(index=df[missing_label_row].index, inplace=True)

    # 3 random rows with a lot of NaNs
    nan_rows = (df['gameDate'].isna() == True)
    df.drop(index=df[nan_rows].index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Adel's addition: any rows that have a gameDuration of 7000 indicate incomplete data.
    duration_not_found = (df['gameDuration'] > 7000)
    df.drop(index=df[duration_not_found].index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    platformId_ser = df['esportsPlatformId']
    df.drop(columns=['esportsPlatformId'],inplace=True)
    col_list = df.columns.tolist()

    # NaN represents a turret was never taken, so it can be replaced with the game duration
    # doing this with a loop because of some setting copy error
    for col in col_list:
        if df[col].isnull().sum() != 0:
            df[col].fillna(df['gameDuration'], inplace=True)

    # some features to look at later
    features_df['winner'] = df['winner']
    features_df['gameDurationMin'] = (df['gameDuration'] / 60)
    features_df['BlueTowerKillsEnd'] = df['BlueTowerKillsEnd']
    features_df['RedTowerKillsEnd'] = df['RedTowerKillsEnd']
    features_df['BlueInhibKillsEnd'] = df['BlueInhibKillsEnd']
    features_df['RedInhibKillsEnd'] = df['RedInhibKillsEnd']
    features_df['BlueBaronKillsEnd'] = df['BlueBaronKillsEnd']
    features_df['RedBaronKillsEnd'] = df['RedBaronKillsEnd']
    features_df['BlueDragonKillsEnd'] = df['BlueDragonKillsEnd']
    features_df['RedDragonKillsEnd'] = df['RedDragonKillsEnd']

    features_df['GoldDiffEnd'] = df['BlueTotalGoldEnd'] - df['RedTotalGoldEnd']

    features_df['VisionScoreBlue'] = df['VisionScoreTopBlue'] + df['VisionScoreJgBlue'] + df['VisionScoreMidBlue'] + df['VisionScoreADBlue'] + df['VisionScoreSupBlue']

    features_df['VisionScoreRed'] = df['VisionScoreTopRed'] + df['VisionScoreJgRed'] + df['VisionScoreMidRed'] + df['VisionScoreADRed'] + df['VisionScoreSupRed']

    #We chose the Assist-to-death ratio in the end, highlighting teamwork.
    #It was statistically significant in testing.
    df['BlueDeathsEndCopy'] = df['BlueDeathsEnd']
    #We don't want to deal with a Division By Zero situation here.
    df['BlueDeathsEnd'].replace(0.0, 1.0, inplace=True)

    features_df['BlueADREnd'] = (df['BlueAssistsEnd'] / df['BlueDeathsEnd'])
    df['BlueDeathsEnd'] = df['BlueDeathsEndCopy']
    df.drop(columns=['BlueDeathsEndCopy'], inplace=True)

    df['RedDeathsEndCopy'] = df['RedDeathsEnd']
    df['RedDeathsEnd'].replace(0.0, 1.0, inplace=True)
    features_df['RedADREnd'] = (df['RedAssistsEnd'] / df['RedDeathsEnd'])
    df['RedDeathsEnd'] = df['RedDeathsEndCopy']
    df.drop(columns=['RedDeathsEndCopy'], inplace=True)

    metrics_df = pd.DataFrame()
    # @ 15 min and @ end
        # gold diff/min
        # ADR, assists/death ratio
    # vision score diff/min
    # objective diff (turrets, inhibs, drakes, barons)
    metrics_df['winner'] = features_df['winner']
    metrics_df['GoldDiffPerMinEnd'] = (features_df['GoldDiffEnd'] / features_df['gameDurationMin'])
    metrics_df['ADRDiffEnd'] = (features_df['BlueADREnd'] - features_df['RedADREnd'])
    metrics_df['VisionScoreDiffPerMin'] = ((features_df['VisionScoreBlue'] - features_df['VisionScoreRed']) / features_df['gameDurationMin'])
    metrics_df['ObjectiveDiff'] = (features_df['BlueTowerKillsEnd'] + 
                                   features_df['BlueInhibKillsEnd'] +
                                   features_df['BlueBaronKillsEnd'] +
                                   features_df['BlueDragonKillsEnd'] + 
                                   features_df['NbRiftHeraldsBlue'] -
                                   features_df['RedTowerKillsEnd'] - 
                                   features_df['RedInhibKillsEnd'] - 
                                   features_df['RedDragonKillsEnd'] - 
                                   features_df['RedBaronKillsEnd'] - 
                                   features_df['NbRiftHeraldsRed'])

    metrics_df['ObjectiveDiff'] = metrics_df['ObjectiveDiff'].astype(int)

    col_list = metrics_df.columns.tolist()
    col_list = col_list[1:]

    scaler = StandardScaler()
    metrics_rescaled = scaler.fit_transform(metrics_df[col_list])
    metrics_rescaled = pd.DataFrame(data=metrics_rescaled,columns=col_list)

    #Adel's quick additions after Sarah delivered the data above, using all the data:
    logreg = LogisticRegression(max_iter = 500,n_jobs=-1)
    y = metrics_df['winner'].apply(lambda x: 1 if x == 'red' else 0)

    #Fitting a quick logistic regression to predict whether the winning team is blue or red:
    #A high score would indicate some viability. (We wound up obtaining at 99% testing score)
    X_train, X_test, y_train, y_test = train_test_split(metrics_rescaled,y,test_size = 0.3)
    logreg.fit(X_train,y_train)

    #Multiply the coefs that we obtained by the standardized metrics
    for index, col in enumerate(metrics_rescaled.columns):
        metrics_rescaled[col]*=logreg.coef_[0,index]

    #Perform the sum to obtain the total score
    metrics_rescaled['gameScore'] = metrics_rescaled.sum(axis=1)

    #Assemble our output
    metrics_rescaled['esportsPlatformId'] = platformId_ser

    return metrics_rescaled[['esportsPlatformId','gameScore']]

if __name__=='__main__':
    df_scores = process_and_score_games()
    df_scores.to_csv('game_scores.csv',sep=';',index=False)
