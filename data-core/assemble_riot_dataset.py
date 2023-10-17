import requests
import json
import gzip
import shutil
import time
import os
from io import BytesIO
import pandas as pd
import datetime as dt

file_error_raised = False

def add_event_to_counter(event_counter, player_index=0, team=None, team_ids_blue=None, team_ids_red=None):
    '''We get an event and assign it to the correct team.
    The code takes under account possible lapses in our data (if we don't have a 'team'
    but have the list of participants).'''

    #At first, we duplicate the list that we're working on and take note of
    #which team needs their counter increased. 0 = blue, 1 = red.
    changed_counter = event_counter
    team_result = team

    #Is our data lacking the team info directly?
    if team_result is None:
        if player_index in team_ids_blue:
            team_result = 'blue'
        elif player_index in team_ids_red:
            team_result = 'red'

    #If it doesn't, or upon fixing it, proceed as normal.
    if team_result is not None:
        if team_result in ['blue','Blue','100']:
            changed_counter[0] = changed_counter[0] + 1
        elif team_result in ['red','Red','200']:
            changed_counter[1] = changed_counter[1] + 1

    return (changed_counter, team_result)

def extract_game_state_data(game_event,value=0):
    '''We process the relevant status updates and add them to dictionaries to
    output. The game event and the list of participants on both teams must be
    provided. By default, without any parameter, the function will treat this
    event as an endgame event depending on the game status. Please specify
    either 10 or 15 on 'value' for those two events.'''
    dict_output = {}
    gameOver = (game_event.get('eventType') == 'game_state_end')
    value = 'End' if gameOver else value #Setting a label for endgame scenarios.

    #Our player data can sometimes be provided in a disorderly way. Let's
    #put some order there. [1,2,3,4,5] = [Top,Jg,Mid,AD,Supp] on blue side.
    #[6,7,8,9,10] will be red side.
    #Note: indexes used will have -1 applied directly to them. (Top = 0)
    player_data = [i for i in game_event.get('participants') if (0 < i.get('participantID') < 11)]
    player_data = sorted(player_data, key = lambda x: x['participantID'])

    if (value == 10 or value == 15) or gameOver:

        #Now let's sample the data overall, from our game state updates:
        gold_top_blue = player_data[0]['totalGold']
        gold_top_red = player_data[5]['totalGold']
        gold_diff_top = gold_top_blue - gold_top_red

        gold_jg_blue = player_data[1]['totalGold']
        gold_jg_red = player_data[6]['totalGold']
        gold_diff_jg = gold_jg_blue - gold_jg_red

        gold_mid_blue = player_data[2]['totalGold']
        gold_mid_red = player_data[7]['totalGold']
        gold_diff_mid = gold_mid_blue - gold_mid_red

        gold_ad_blue = player_data[3]['totalGold']
        gold_ad_red = player_data[8]['totalGold']
        gold_diff_ad = gold_ad_blue - gold_ad_red

        gold_sup_blue = player_data[4]['totalGold']
        gold_sup_red = player_data[9]['totalGold']
        gold_diff_sup = gold_sup_blue - gold_sup_red

        xp_diff_top = player_data[0]['XP'] - player_data[5]['XP']
        xp_diff_jg = player_data[1]['XP'] - player_data[6]['XP']
        xp_diff_mid = player_data[2]['XP'] - player_data[7]['XP']
        xp_diff_ad = player_data[3]['XP'] - player_data[8]['XP']
        xp_diff_sup = player_data[4]['XP'] - player_data[9]['XP']
        dict_output.update({
            f'GoldTopBlue{value}': gold_top_blue,
            f'GoldJgBlue{value}': gold_jg_blue,
            f'GoldMidBlue{value}': gold_mid_blue,
            f'GoldADBlue{value}': gold_ad_blue,
            f'GoldSupBlue{value}': gold_sup_blue,
            f'GoldTopRed{value}': gold_top_red,
            f'GoldJgRed{value}': gold_jg_red,
            f'GoldMidRed{value}': gold_mid_red,
            f'GoldADRed{value}': gold_ad_red,
            f'GoldSupRed{value}': gold_sup_red,
            f'GoldDiff{value}Top': gold_diff_top,
            f'GoldDiff{value}Jg': gold_diff_jg,
            f'GoldDiff{value}Mid': gold_diff_mid,
            f'GoldDiff{value}AD': gold_diff_ad,
            f'GoldDiff{value}Bot': gold_diff_sup,
            f'XPDiff{value}Top': xp_diff_top,
            f'XPDiff{value}Jg': xp_diff_jg,
            f'XPDiff{value}Mid': xp_diff_mid,
            f'XPDiff{value}AD': xp_diff_ad,
            f'XPDiff{value}Bot': xp_diff_sup,
        })
        #try:
        if game_event.get('teams',None) is None:
            blue_kills, red_kills = game_event['blue']['championsKills'], game_event['red']['championsKills']
            blue_assists, red_assists = game_event['blue']['assists'], game_event['red']['assists']
            blue_deaths, red_deaths = game_event['blue']['deaths'], game_event['red']['deaths']
            blue_gold, red_gold = game_event['blue']['totalGold'], game_event['red']['totalGold']
            blue_tower_kills, red_tower_kills = game_event['blue']['towerKills'], game_event['red']['towerKills']
            blue_dragon_kills, red_dragon_kills = game_event['blue']['dragonKills'], game_event['red']['dragonKills']
        else:
            blue_kills, red_kills = game_event['teams'][0]['championsKills'], game_event['teams'][1]['championsKills']
            blue_assists, red_assists = game_event['teams'][0]['assists'], game_event['teams'][1]['assists']
            blue_deaths, red_deaths = game_event['teams'][0]['deaths'], game_event['teams'][1]['deaths']
            blue_gold, red_gold = game_event['teams'][0]['totalGold'], game_event['teams'][1]['totalGold']
            blue_tower_kills, red_tower_kills = game_event['teams'][0]['towerKills'], game_event['teams'][1]['towerKills']
            blue_dragon_kills, red_dragon_kills = game_event['teams'][0]['dragonKills'], game_event['teams'][1]['dragonKills']

        dict_output.update({
            f'BlueKills{value}': blue_kills,
            f'BlueAssists{value}': blue_assists,
            f'BlueDeaths{value}': blue_deaths,
            f'BlueTotalGold{value}': blue_gold,
            f'BlueDragonKills{value}': blue_dragon_kills,
            f'BlueTowerKills{value}': blue_tower_kills,
            f'RedKills{value}': red_kills,
            f'RedAssists{value}': red_assists,
            f'RedDeaths{value}': red_deaths,
            f'RedTotalGold{value}': red_gold,
            f'RedDragonKills{value}': red_dragon_kills,
            f'RedTowerKills{value}': red_tower_kills
        })

        if value != 10:
            if game_event.get('teams',None) is None:
                blue_inhib_kills, red_inhib_kills = game_event['blue']['inhibKills'], game_event['red']['inhibKills']
            else:
                blue_inhib_kills, red_inhib_kills = game_event['teams'][0]['inhibKills'], game_event['teams'][1]['inhibKills']

            dict_output.update({
                f'BlueInhibKills{value}': blue_inhib_kills,
                f'RedInhibKills{value}': red_inhib_kills
            })
        #except:
            #print('We could not extract specific status update values in this file.')



        if gameOver:
            if game_event.get('teams',None) is None:
                blue_baron_kills, red_baron_kills = game_event['blue']['baronKills'], game_event['red']['baronKills']
            else:
                blue_baron_kills, red_baron_kills = game_event['teams'][0]['baronKills'], game_event['teams'][1]['baronKills']
            dict_output.update({
                f'BlueBaronKills{value}': blue_baron_kills,
                f'RedBaronKills{value}': red_baron_kills
            })

            blue_vision_score_top = player_data[0]['VISION_SCORE']
            blue_vision_score_jg = player_data[1]['VISION_SCORE']
            blue_vision_score_mid = player_data[2]['VISION_SCORE']
            blue_vision_score_ad = player_data[3]['VISION_SCORE']
            blue_vision_score_sup = player_data[4]['VISION_SCORE']
            red_vision_score_top = player_data[5]['VISION_SCORE']
            red_vision_score_jg = player_data[6]['VISION_SCORE']
            red_vision_score_mid = player_data[7]['VISION_SCORE']
            red_vision_score_ad = player_data[8]['VISION_SCORE']
            red_vision_score_sup = player_data[9]['VISION_SCORE']

            dict_output.update({
                'VisionScoreTopBlue': blue_vision_score_top,
                'VisionScoreJgBlue': blue_vision_score_jg,
                'VisionScoreMidBlue': blue_vision_score_mid,
                'VisionScoreADBlue': blue_vision_score_ad,
                'VisionScoreSupBlue': blue_vision_score_sup,
                'VisionScoreTopRed': red_vision_score_top,
                'VisionScoreJgRed': red_vision_score_jg,
                'VisionScoreMidRed': red_vision_score_mid,
                'VisionScoreADRed': red_vision_score_ad,
                'VisionScoreSupRed': red_vision_score_sup
            })

            blue_damage_dealt_top = player_data[0]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            blue_damage_dealt_jg = player_data[1]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            blue_damage_dealt_mid = player_data[2]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            blue_damage_dealt_ad = player_data[3]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            blue_damage_dealt_sup = player_data[4]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            red_damage_dealt_top = player_data[5]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            red_damage_dealt_jg = player_data[6]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            red_damage_dealt_mid = player_data[7]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            red_damage_dealt_ad = player_data[8]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']
            red_damage_dealt_sup = player_data[9]['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS']

            dict_output.update({
                'DamageDealtTopBlue': blue_damage_dealt_top,
                'DamageDealtJgBlue': blue_damage_dealt_jg,
                'DamageDealtMidBlue': blue_damage_dealt_mid,
                'DamageDealtADBlue': blue_damage_dealt_ad,
                'DamageDealtSupBlue': blue_damage_dealt_sup,
                'DamageDealtTopRed': red_damage_dealt_top,
                'DamageDealtJgRed': red_damage_dealt_jg,
                'DamageDealtMidRed': red_damage_dealt_mid,
                'DamageDealtADRed': red_damage_dealt_ad,
                'DamageDealtSupRed': red_damage_dealt_sup
            })

            blue_damage_taken_top = player_data[0]['TOTAL_DAMAGE_TAKEN']
            blue_damage_taken_jg = player_data[1]['TOTAL_DAMAGE_TAKEN']
            blue_damage_taken_mid = player_data[2]['TOTAL_DAMAGE_TAKEN']
            blue_damage_taken_ad = player_data[3]['TOTAL_DAMAGE_TAKEN']
            blue_damage_taken_sup = player_data[4]['TOTAL_DAMAGE_TAKEN']
            red_damage_taken_top = player_data[5]['TOTAL_DAMAGE_TAKEN']
            red_damage_taken_jg = player_data[6]['TOTAL_DAMAGE_TAKEN']
            red_damage_taken_mid = player_data[7]['TOTAL_DAMAGE_TAKEN']
            red_damage_taken_ad = player_data[8]['TOTAL_DAMAGE_TAKEN']
            red_damage_taken_sup = player_data[9]['TOTAL_DAMAGE_TAKEN']

            dict_output.update({
                'DamageTakenTopBlue': blue_damage_taken_top,
                'DamageTakenJgBlue': blue_damage_taken_jg,
                'DamageTakenMidBlue': blue_damage_taken_mid,
                'DamageTakenADBlue': blue_damage_taken_ad,
                'DamageTakenSupBlue': blue_damage_taken_sup,
                'DamageTakenTopRed': red_damage_taken_top,
                'DamageTakenJgRed': red_damage_taken_jg,
                'DamageTakenMidRed': red_damage_taken_mid,
                'DamageTakenADRed': red_damage_taken_ad,
                'DamageTakenSupRed': red_damage_taken_sup
            })

            blue_cc_score_top = player_data[0]['TIME_CCING_OTHERS']
            blue_cc_score_jg = player_data[1]['TIME_CCING_OTHERS']
            blue_cc_score_mid = player_data[2]['TIME_CCING_OTHERS']
            blue_cc_score_ad = player_data[3]['TIME_CCING_OTHERS']
            blue_cc_score_sup = player_data[4]['TIME_CCING_OTHERS']
            red_cc_score_top = player_data[5]['TIME_CCING_OTHERS']
            red_cc_score_jg = player_data[6]['TIME_CCING_OTHERS']
            red_cc_score_mid = player_data[7]['TIME_CCING_OTHERS']
            red_cc_score_ad = player_data[8]['TIME_CCING_OTHERS']
            red_cc_score_sup = player_data[9]['TIME_CCING_OTHERS']

            dict_output.update({
                'TotalCCDurationTopBlue': blue_cc_score_top,
                'TotalCCDurationJgBlue': blue_cc_score_jg,
                'TotalCCDurationMidBlue': blue_cc_score_mid,
                'TotalCCDurationADBlue': blue_cc_score_ad,
                'TotalCCDurationSupBlue': blue_cc_score_sup,
                'TotalCCDurationTopRed': red_cc_score_top,
                'TotalCCDurationJgRed': red_cc_score_jg,
                'TotalCCDurationMidRed': red_cc_score_mid,
                'TotalCCDurationADRed': red_cc_score_ad,
                'TotalCCDurationSupRed': red_cc_score_sup
            })

    return dict_output

def extract_datapoints_from_game(game_json):
    '''We are extracting every single datapoint that our extraction process
    allows us to claim. That said, we might have to modify this code if we
    end up extracting more data later.'''
    global file_error_raised
    dict_stats = {}
    game_info_dict = {}
    game_state_10 = {}
    game_state_15 = {}
    game_state_end = {}
    blue_participant_id = []
    red_participant_id = []
    header_stats = None
    wards_placed = [0 for i in range(0,10)]
    control_wards_placed = [0 for i in range(0,10)]
    wards_killed = [0 for i in range(0,10)]
    control_wards_killed = [0 for i in range(0,10)]
    current_event = None
    previous_event = None
    game_duration = 7200 #Put it at two hours for funsies.
    game_winner = None
    dragon_type_queued = [None, None]
    dragon_index = 0
    dragon_soul_taken = False
    dragon_soul = {
        'DragonSoulTimer':None,
        'DragonSoulType':None,
        'DragonSoulTaker':None
    }
    elder_dragons_taken = [0, 0]
    plates_taken = [0, 0]
    own_camps_taken = [0, 0] #blue, red
    enemy_camps_taken = [0, 0] #blue, red
    rift_herald_count = [0, 0] #blue, red
    scuttle_count = [0, 0] #blue, red
    dragon_count = [0, 0] #blue, red
    dragon_type_count = {'blue':{}, 'red':{}}
    baron_count = [0, 0] #blue, red
    tower_count = [0, 0] #blue, red
    tower_log = {
        'OuterTopBlueTimer': None,
        'OuterMidBlueTimer': None,
        'OuterBotBlueTimer': None,
        'InnerTopBlueTimer': None,
        'InnerMidBlueTimer': None,
        'InnerBotBlueTimer': None,
        'BaseTopBlueTimer': None,
        'BaseMidBlueTimer': None,
        'BaseBotBlueTimer': None,
        'Nexus1MidBlueTimer': None,
        'Nexus2MidBlueTimer': None,
        'OuterTopRedTimer': None,
        'OuterMidRedTimer': None,
        'OuterBotRedTimer': None,
        'InnerTopRedTimer': None,
        'InnerMidRedTimer': None,
        'InnerBotRedTimer': None,
        'BaseTopRedTimer': None,
        'BaseMidRedTimer': None,
        'BaseBotRedTimer': None,
        'Nexus1MidRedTimer': None,
        'Nexus2MidRedTimer': None
    } #Time values for each event
    team_side = None

    smaller_camps = ['blueCamp','redCamp','gromp','wolf','krug','raptor']

    #Data integrity check, but also getting relevant data.

    try:
        game_duration = game_json[-1].get('gameTime')
        game_winner = game_json[-1].get('winningTeam')
    except:
        file_error_raised = True
        print("The game's data does not contain the entirety of the game.")
        #TO_DO: plug Tim's game_end timestamp (game duration)

    for game_event in game_json:

        # In case we have our intro header somewhere in there:
        if game_event.get('eventType',None) is None:

            try:
                game_date = dt.datetime.fromisoformat(game_event.get('gameDate',None))
                esports_platform = game_event.get('esportsPlatformId',None)
                game_patch = game_event.get('gameVersion',None)
                header_stats = {
                    'gameDate': game_date,
                    'esportsPlatformId': esports_platform,
                    'gameVersion': game_patch,
                    'gameDuration': game_duration
                }
            except:
                file_error_raised = True
        else:
            current_event = game_event.get('eventType',None)

        # Important bit: we get our game info.
        if game_event.get('eventType') == 'game_info':
            blue_participant_id = []
            red_participant_id = []
            blue_summoner_name = []
            red_summoner_name = []
            blue_champion = []
            red_champion = []

            blue_data = game_event.get('blue')
            for player in blue_data:
                blue_participant_id.append(player.get('participantID'))
                blue_summoner_name.append(player.get('summonerName'))
                blue_champion.append(player.get('championName'))

            red_data = game_event.get('red')
            for player in red_data:
                red_participant_id.append(player.get('participantID'))
                red_summoner_name.append(player.get('summonerName'))
                red_champion.append(player.get('championName'))

        # Turret plates: let's log them
        if game_event.get('eventType') == 'turret_plate_destroyed':
            #We record whoever took the plate
            killer_team = game_event.get('team')
            plates_taken = add_event_to_counter(plates_taken, None, killer_team,
                                                    None, None)[0]

        # Wards placed (Experimental)
        if game_event.get('eventType') == 'ward_placed':
            placer = game_event.get('placer')
            #location = game_event.get('position') #Unused for now
            ward_type = game_event.get('wardType')

            #We verify that the ward isn't placed when the game is nearly done
            #Alternatively, we could check that the ward isn't placed within a
            #set radius of the base or nexus. Said radius must reflect that the ward
            #is useless. That said, we won't implement that for now, for lack
            #of knowledge.
            if game_event.get('gameTime') < game_duration-20:
                #Is our ward a control ward?
                #Keep in mind: indexes start from 0, the -1 adapts it all.
                if ward_type == 'control':
                    control_wards_placed[placer-1] = control_wards_placed[placer-1] + 1
                else:
                    wards_placed[placer-1] = wards_placed[placer-1] + 1

        if game_event.get('eventType') == 'ward_killed':
            killer = game_event.get('killer')
            #location = game_event.get('position') #Unused for now
            ward_type = game_event.get('wardType')

            #Wards killed - let's record the ones that weren't spammed near endgame
            if game_event.get('gameTime') < game_duration-20:
                #Is our ward a control ward?
                #Keep in mind: indexes start from 0, the -1 adapts it all.
                if ward_type == 'control':
                    control_wards_killed[killer-1] = control_wards_killed[killer-1] + 1
                else:
                    wards_killed[killer-1] = wards_killed[killer-1] + 1

        if game_event.get('eventType') == 'queued_dragon_info':
            dragon_index = dragon_index + 1
            if dragon_type_queued[0] is None:
                dragon_type_queued[0] = game_event.get('nextDragonName')
            else:
                dragon_type_queued[1] = game_event.get('nextDragonName')

        if game_event.get('eventType') == 'epic_monster_kill':
            #So, there are A LOT of epic monsters killed.
            #The events have the "killer" in common.
            killer = game_event.get('killer')
            killer_team = game_event.get('team')

            #Step 1: counterjungle tally
            if game_event.get('inEnemyJungle'):
                #Temporary workaround given data mistakes were made
                enemy_camps_taken = add_event_to_counter(enemy_camps_taken, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]
            #If no counterjungling is afoot, take this number instead:
            elif game_event.get('monsterType') in smaller_camps:
                own_camps_taken = add_event_to_counter(own_camps_taken, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]

            #Step 2: Scuttles, Heralds, Barons, and Dragons
            if game_event.get('monsterType') == 'scuttleCrab':
                scuttle_count = add_event_to_counter(scuttle_count, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]

            if game_event.get('monsterType') == 'riftHerald':
                rift_herald_count = add_event_to_counter(rift_herald_count, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]

            if game_event.get('monsterType') == 'baron':
                baron_count = add_event_to_counter(baron_count, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]

            if game_event.get('monsterType') == 'dragon':
                dragon_count, team_side = add_event_to_counter(dragon_count, killer, killer_team,
                                                        blue_participant_id, red_participant_id)
                if team_side is not None:
                    side_select = {'blue': 0, 'red':1}
                    if not dragon_soul_taken:
                        i = side_select.get(team_side)
                        nb_dragons = dragon_count[i]
                        if nb_dragons == 4:
                            dragon_soul_taken = True
                            dragon_soul.update({
                                'DragonSoulTimer':game_event.get('gameTime'),
                                'DragonSoulType':dragon_type_queued[0],
                                'DragonSoulTeam':team_side
                            })
                    else:
                        elder_dragons_taken = add_event_to_counter(elder_dragons_taken, killer, killer_team,
                                                        blue_participant_id, red_participant_id)[0]


                    #dragon_type_count[team_side].update({dragon_number+1 : [game_event.get('gameTime'), dragon_type_queued[0]]})

                if dragon_type_queued[1] is not None:
                    dragon_type_queued[0] = dragon_type_queued[1]
                    dragon_type_queued[1] = None

        if game_event.get('eventType') == 'building_destroyed':
            try:
                team_side = game_event.get('team',None)
                building_type = game_event.get('buildingType')

                if building_type == 'turret' and team_side is not None:
                    building_lane = game_event.get('lane').title()
                    building_tier = game_event.get('turretTier').title()

                    if building_tier == 'Nexus':
                        if tower_log.get(f'{building_tier}1{building_lane}{team_side.title()}Timer',None) is not None:
                            building_tier = 'Nexus2'
                        else:
                            building_tier = 'Nexus1'

                    tower_log.update({f'{building_tier}{building_lane}{team_side.title()}Timer':
                                    game_event.get('gameTime')})
                    tower_count = add_event_to_counter(tower_count, None, team_side,
                                                        None, None)[0]
            except:
                print('Did a building self-destruct by any chance? Something has gone awry here.')

        if game_event.get('eventType') == 'game_state_10mn':
            game_state_10 = extract_game_state_data(game_event,10)

        if game_event.get('eventType') == 'game_state_15mn':
            game_state_15 = extract_game_state_data(game_event,15)

        if game_event.get('eventType') == 'game_state_end':
            game_state_end = extract_game_state_data(game_event)

        previous_event = current_event


    #This will be our first entry in the dictionaries.
    if header_stats is not None:
        dict_stats.update(header_stats)

    dict_stats.update({
        'NbWardsPlacedBlue': sum(wards_placed[i-1] for i in blue_participant_id),
        'NbWardsPlacedRed': sum(wards_placed[i-1] for i in red_participant_id),
        'NbControlWardsPlacedBlue': sum(control_wards_placed[i-1] for i in blue_participant_id),
        'NbControlWardsPlacedRed': sum(control_wards_placed[i-1] for i in red_participant_id),
        'NbWardsKilledBlue': sum(wards_killed[i-1] for i in blue_participant_id),
        'NbWardsKilledRed': sum(wards_killed[i-1] for i in red_participant_id),
        'NbControlWardsKilledBlue': sum(control_wards_killed[i-1] for i in blue_participant_id),
        'NbControlWardsKilledRed':sum(control_wards_killed[i-1] for i in red_participant_id)
    })

    dict_stats.update({
        'NbCampsSecuredBlue': own_camps_taken[0],
        'NbCampsSecuredRed': own_camps_taken[1],
        'NbCampsStolenBlue': enemy_camps_taken[0],
        'NbCampsStolenRed': enemy_camps_taken[1],
        'NbScuttlesBlue': scuttle_count[0],
        'NbScuttlesRed': scuttle_count[1],
        'NbRiftHeraldsBlue': rift_herald_count[0],
        'NbRiftHeraldsRed': rift_herald_count[1],
        'NbDragonsBlue': dragon_count[0]-elder_dragons_taken[0], #Denotes only regular dragons
        'NbDragonsRed': dragon_count[1]-elder_dragons_taken[1],
        'NbBaronsBlue': baron_count[0],
        'NbBaronsRed': baron_count[1],
        'NbEldersBlue': elder_dragons_taken[0],
        'NbEldersRed': elder_dragons_taken[1],
        'NbTowersBlue': tower_count[0],
        'NbTowersRed': tower_count[1],
        'NbPlatesBlue': plates_taken[0],
        'NbPlatesRed': plates_taken[1]
    })

    dict_stats.update(tower_log)
    dict_stats.update(dragon_soul)
    dict_stats.update(game_state_10)
    dict_stats.update(game_state_15)
    dict_stats.update(game_state_end)
    dict_stats.update({'winner': game_winner})

    return dict_stats

def build_csv():
    games_dir = 'games'
    cleaned_files = os.listdir(games_dir)
    total_data = []
    global file_error_raised

    for cleaned_game in cleaned_files:
        with open(f'{games_dir}/{cleaned_game}','r') as game_file:
            game_data = json.load(game_file)
            if file_error_raised:
                print(f'{games_dir}/{cleaned_game} requires inspection due to missing data.')
                file_error_raised = False
            #try:
            total_data.append(extract_datapoints_from_game(game_data))
            #except:
                #print(f'Missing values on {games_dir}/{cleaned_game}')
                #continue

    df_data = pd.DataFrame(total_data)
    df_data.to_csv(path_or_buf='hackathon-riot-data.csv',sep=';',index=False)

def map_all_games():
    with open("esports-data/tournaments-cleaned.json", "r") as json_file:
       tournaments_data = json.load(json_file)
    with open("esports-data/mapping_data.json", "r") as json_file:
       mappings_data = json.load(json_file)



    mappings = {
       esports_game["esportsGameId"]: esports_game for esports_game in mappings_data
    }

if __name__  == '__main__':
    build_csv()
