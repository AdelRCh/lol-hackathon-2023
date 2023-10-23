import pandas as pd
import numpy as np
#Just in case:
from game_scoring import process_and_score_games
import math

def k_factor (k_score, gameScore):
    '''How much can we adjust our K-score? Things are much simpler executed
    in a function. As we apply the logic from the Football Elo Ratings to
    League of Legends, we will figure out a way to amplify our score by a
    bit.'''
    adj_factor = gameScore/10
    if (adj_factor*1.33157 + 0.358061 > 0):
        try:
            #If we can calculate how our game score affects things,
            res = (0.682321)*np.log(adj_factor*1.33157 + 0.358061) + 0.696658

            #Epic throw outlier incoming? Extraordinarily close game?
            #We limit the amount of point a team
            #is denied to 80% maximum.
            if res < -0.8:
                res = -0.8

            return k_score*(1+res)

        #Now, if we can't calculate the formula above, obtained from that regression
        #because res screams about a negative value in a log:
        except:
            return k_score/5
            #we're dealing with an outlier case where there were
            #rather epic throws, but we still need a winner.
            #Very few points to be won here. The audience lost the game and some
            #braincells.
    return k_score/5

def elo_formula(blue_id, red_id, winner, game_date, k_score,
                tour_label, tourney, stage_name, year,league_id,
                crush_score=0, blue_rating=1200,  red_rating=1200,
                game_score=0, bo3=False, bo5=False,
                game_num=1):
    '''The function calculates and updates Elo ratings for all known games.
    All teams will start at 1200 Elo in our formula, where we define "1200"
    as a baseline score. Over time, the scores will evolve to reflect a team's
    current rating.

    At this point, we don't know whether crush score can be implemented, or if
    we will take bo3/bo5 status under account.'''

    res_dict = {
        'blue':(1,0),
        'red':(0,1)
    }

    if type(winner) == str:
        result_blue, result_red = res_dict.get(winner.lower())
    elif type(winner) == int:
        result_blue, result_red = (1-winner, winner)

    if result_blue == 1:
        game_score = -game_score #Flip the sign, get the true game score.
        #Blue victories from our scoring system are usually negative numbers,
        #unless the red team threw intergalactically hard. (A team can win
        #despite a negative score)

    #The K-score gets a pretty brutal adjustment depending on the game score.
    adjusted_k_score = k_factor(k_score,game_score)

    rating_delta_blue = blue_rating - red_rating
    rating_delta_red = -rating_delta_blue
    expected_result_blue = 1/(10**(-rating_delta_blue/400) + 1)
    expected_result_red = 1/(10**(-rating_delta_red/400) + 1)
    new_rating_blue = blue_rating + (adjusted_k_score * (result_blue - expected_result_blue))
    new_rating_red = red_rating + (adjusted_k_score * (result_red - expected_result_red))

    res_blue = {
        'leagueId':league_id,
        'leagueLabel': tour_label,
        'stageTournament': tourney,
        'stageName': stage_name,
        'year': year,
        'date':game_date,
        'game_num':game_num,
        'team': blue_id,
        'rating': new_rating_blue
    }
    res_red = {
        'leagueId':league_id,
        'leagueLabel': tour_label,
        'stageTournament': tourney,
        'stageName': stage_name,
        'year': year,
        'date':game_date,
        'game_num':game_num,
        'team': red_id,
        'rating': new_rating_red}

    return res_blue, res_red

if __name__=='__main__':
    year_selected = "2020"

    dirname_scores = '' #Change to reflect their location
    dirname_gameevents = '' #Change to '<dirname>/' to reflect their location
    dirname_gamelist = '' #Same as above
    #game_data = pd.read_csv('hackathon-riot-data.csv',sep=';').reset_index(drop=True)
    #game_data = game_data[['gameDate','esportsPlatformId','gameVersion']]
    #team_id_df = pd.read_csv('all_recorded_games_with_riot_data.csv').reset_index(drop=True)
    #scores_df = pd.read_csv('game_scores.csv',sep=';')

    #team_id_df.rename(columns={'esportsGameId':'esportsPlatformId'},inplace=True)
    #final_df = scores_df.merge(team_id_df,how='left',left_on='esportsPlatformId',right_on='esportsGameId')
    #final_df = final_df.merge(game_data,how='left',on='esportsPlatformId')
    #final_df.sort_values(by='gameDate',ascending=True,inplace=True)
    final_df = pd.read_csv('process_everything.csv',sep=';')
    final_df = final_df[final_df.gameDate.str.startswith(year_selected)]

    #print(final_df.columns)

    rating_list = []
    elo_log = {}

    elo_tiers = {
        10: 1200,
        20: 1500,
        30: 1800,
        40: 2100,
        50: 2100,
        60: 2100
    }

    for index, row in final_df.iterrows():
        #Starting values at 1200 for elos
        old_blue_rating = elo_log.get(row['blue'],elo_tiers.get(row['kScore'],1200))
        old_red_rating = elo_log.get(row['red'],elo_tiers.get(row['kScore'],1200))

        elo_blue, elo_red = elo_formula(row.blue, row.red, \
            row.winner, row.gameDate, (row.kScore * row.kMult), row.leagueLabel,\
            row.stageTournament, row.stageRound, year_selected,row.leagueId, 0, old_blue_rating, \
            old_red_rating, row.gameScore, False, False, row.gameNumber)
        elo_log.update({
            row.blue:elo_blue.get('rating',None),
            row.red:elo_red.get('rating',None)
            })
        rating_list.append(elo_blue)
        rating_list.append(elo_red)
        #except:
            #continue

    df_rating = pd.DataFrame(rating_list)
    df_rating.to_csv(f'elos_{year_selected}.csv',sep=';',index=False)
