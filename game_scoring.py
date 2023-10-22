import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

def process_and_score_games(filename='hackathon-riot-data.csv', lplfilename='oracles_elixir_lpl_data.csv'):
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
    # objective diff (turrets, inhibs, drakes, barons, heralds are omitted because missing in LPL data)
    metrics_df['winner'] = features_df['winner']
    metrics_df['GoldDiffPerMinEnd'] = (features_df['GoldDiffEnd'] / features_df['gameDurationMin'])
    metrics_df['ADRDiffEnd'] = (features_df['BlueADREnd'] - features_df['RedADREnd'])
    metrics_df['VisionScoreDiffPerMin'] = ((features_df['VisionScoreBlue'] - features_df['VisionScoreRed']) / features_df['gameDurationMin'])
    metrics_df['ObjectiveDiff'] = (features_df['BlueTowerKillsEnd'] + 
                                   features_df['BlueInhibKillsEnd'] +
                                   features_df['BlueBaronKillsEnd'] +
                                   features_df['BlueDragonKillsEnd'] -
                                   features_df['RedTowerKillsEnd'] - 
                                   features_df['RedInhibKillsEnd'] - 
                                   features_df['RedDragonKillsEnd'] - 
                                   features_df['RedBaronKillsEnd'])

    metrics_df['ObjectiveDiff'] = metrics_df['ObjectiveDiff'].astype(int)

    # integrating LPL data
    lpl_df = pd.read_csv(lplfilename, sep=';')

    # 1 indicates a win for blue result and red result, so a winner column can be obtained
    lpl_df['BlueResult'].replace(1, 'blue', inplace=True)
    lpl_df['BlueResult'].replace(0, 'red', inplace=True)
    lpl_df.rename(columns={'BlueResult':'winner'}, inplace=True) 
    lpl_df.drop(columns=['RedResult'], inplace=True)

    # dataframe to collect necessary columns to calculate the 4 metrics
    lpl_features_df = pd.DataFrame()
    lpl_features_df['winner'] = lpl_df['winner']

    col_list = features_df.columns.tolist()
    lpl_col_list = lpl_df.columns.tolist()

    # copy pasta-ing columns
    for col in col_list:
        if col in lpl_col_list:
            lpl_features_df[col] = lpl_df[col]
        else:
            continue
            
    # calculating other columns used in the metrics
    lpl_features_df['gameDurationMin'] = (lpl_df['gameDuration'] / 60)
    lpl_features_df['GoldDiffEnd'] = lpl_df['GoldDiffEndTop'] + lpl_df['GoldDiffEndJg'] + lpl_df['GoldDiffEndMid'] + lpl_df['GoldDiffEndAD'] + lpl_df['GoldDiffEndSup']
    lpl_features_df['VisionScoreBlue'] = lpl_df['VisionScoreTopBlue'] + lpl_df['VisionScoreJgBlue'] + lpl_df['VisionScoreMidBlue'] + lpl_df['VisionScoreADBlue'] + lpl_df['VisionScoreSupBlue']
    lpl_features_df['VisionScoreRed'] = lpl_df['VisionScoreTopRed'] + lpl_df['VisionScoreJgRed'] + lpl_df['VisionScoreMidRed'] + lpl_df['VisionScoreADRed'] + lpl_df['VisionScoreSupRed']

    # ADR, assists/deaths ratio
    lpl_df['BlueDeathsEndCopy'] = lpl_df['BlueDeathsEnd']
    lpl_df['BlueDeathsEnd'].replace(0.0, 1.0, inplace=True)
    lpl_features_df['BlueADREnd'] = (lpl_df['BlueAssistsEnd'] / lpl_df['BlueDeathsEnd'])
    lpl_df['BlueDeathsEnd'] = lpl_df['BlueDeathsEndCopy']
    lpl_df.drop(columns=['BlueDeathsEndCopy'], inplace=True)

    lpl_df['RedDeathsEndCopy'] = lpl_df['RedDeathsEnd']
    lpl_df['RedDeathsEnd'].replace(0.0, 1.0, inplace=True)
    lpl_features_df['RedADREnd'] = (lpl_df['RedAssistsEnd'] / lpl_df['RedDeathsEnd'])
    lpl_df['RedDeathsEnd'] = lpl_df['RedDeathsEndCopy']
    lpl_df.drop(columns=['RedDeathsEndCopy'], inplace=True)

    lpl_metrics_df = pd.DataFrame()
    # @ end
        # gold diff/min
        # ADR, assists/death ratio
    # vision score diff/min
    # objective diff (turrets, inhibs, drakes, barons)
    lpl_metrics_df['winner'] = lpl_features_df['winner']
    lpl_metrics_df['GoldDiffPerMinEnd'] = (lpl_features_df['GoldDiffEnd'] / lpl_features_df['gameDurationMin'])
    lpl_metrics_df['ADRDiffEnd'] = (lpl_features_df['BlueADREnd'] - lpl_features_df['RedADREnd'])
    lpl_metrics_df['VisionScoreDiffPerMin'] = ((lpl_features_df['VisionScoreBlue'] - lpl_features_df['VisionScoreRed']) / lpl_features_df['gameDurationMin'])
    lpl_metrics_df['ObjectiveDiff'] = (lpl_features_df['BlueTowerKillsEnd'] + 
                                lpl_features_df['BlueInhibKillsEnd'] + 
                                lpl_features_df['BlueBaronKillsEnd'] + 
                                lpl_features_df['BlueDragonKillsEnd'] -
                                lpl_features_df['RedTowerKillsEnd'] -
                                lpl_features_df['RedInhibKillsEnd'] - 
                                lpl_features_df['RedBaronKillsEnd'] -
                                lpl_features_df['RedDragonKillsEnd'])

    lpl_metrics_df['ObjectiveDiff'] = lpl_metrics_df['ObjectiveDiff'].astype(int)

    # concatenates LPL dataframe to the main dataframe
    metrics_df = pd.concat([metrics_df, lpl_metrics_df], ignore_index=True)

    # these two features are omitted because LPL data is missing them
    metrics_df.drop(columns=['GoldDiffPerMin15', 'ADRDiff15'], inplace=True)

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
