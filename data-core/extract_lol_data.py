import requests
import json
import gzip
import shutil
import time
import os
from io import BytesIO
import pandas as pd
import datetime as dt

S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com"

def download_gzip_and_write_to_json(file_name):
   '''
   We download the gzip with the corresponding <file_name>
   and write its contents in a JSON format. Taken from Riot's supplied code.
   '''
   local_file_name = file_name.replace(":", "_")
   # If file already exists locally do not re-download game
   if os.path.isfile(f"{local_file_name}.json"):
       return

   response = requests.get(f"{S3_BUCKET_URL}/{file_name}.json.gz")
   if response.status_code == 200:
       try:
           gzip_bytes = BytesIO(response.content)
           with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
               with open(f"{local_file_name}.json", 'wb') as output_file:
                   shutil.copyfileobj(gzipped_file, output_file)
               print(f"{file_name}.json written")
       except Exception as e:
           print("Error:", e)
   else:
       print(f"Failed to download {file_name}")

def download_esports_files():
   '''
   Initial step: we download all the files that contain our data for future use.
   '''
   directory = "esports-data"
   if not os.path.exists(directory):
       os.makedirs(directory)

   esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]
   for file_name in esports_data_files:
       download_gzip_and_write_to_json(f"{directory}/{file_name}")

def filter_leagues():
    '''
    This function pre-emptively cleans the leagues.json file if it has just been
    downloaded, or if we have deleted it before a restart.
    '''
    #If we haven't pre-cleaned things:
    if not os.path.isfile('esports-data/leagues-cleaned.json'):
        leagues_df = pd.read_json('esports-data/leagues.json')
        #Clear TFT and All-Star tournaments from our leagues
        try:
            drop_these = leagues_df[(leagues_df.id == 98767991295297328) |
                                          (leagues_df.id == 108001239847565216)]
            leagues_df.drop(axis=0, labels=drop_these.index, inplace=True)
        except:
            print('We have already cleared the rows related to All-Star and TFT.')
        #Drop extraneous columns
        try:
            leagues_df.drop(axis=1, columns=['displayPriority','image','sport',
                                            'lightImage','darkImage','slug'],
                            inplace=True)
        except:
            print('We have already cleared the columns that we did not need.')
        leagues_df.to_json('esports-data/leagues-cleaned.json')

def filter_tournaments():
    '''
    This function pre-emptively cleans the tournaments.json file if it has just
    been downloaded, or if we have deleted it before a restart.
    It will always run after filter_leagues.
    '''
    if not os.path.isfile('esports-data/tournaments-cleaned.json'):
        leagues_df = pd.read_json('esports-data/leagues.json')
        tournaments_df = pd.read_json('esports-data/tournaments.json')
        # We take a look at the Regional League codes and keep only the
        # tournaments that fit those competitions. Because tournament IDs don't
        # carry over metadata tables, we have to do it this way for now.
        # We will sort tournaments from latest to earliest.
        tournaments_df = pd.read_json('esports-data/tournaments.json')
        leagues_df = leagues_df[['id','name']].rename(columns={'id':'leagueId',
                                                        'name':'leagueName'})
        clean_trs_df = pd.merge(tournaments_df,leagues_df,how='inner',on='leagueId')

        # Although the OPL tournaments are no longer "relevant," they would be
        # extremely useful when analyzing data from 2020 given its Worlds status
        # at the time.
        opl_df = tournaments_df[(tournaments_df['id'] == 104151038596540368)
                                | (tournaments_df['id'] == 103535401218775280)].copy()
        # LCO takes over where the OPL left (continuity)
        opl_df.loc[:,'leagueName'] = 'LCO'

        # Let there be koalas.
        full_df = pd.concat([clean_trs_df,opl_df])
        full_df.sort_values(by='startDate',ascending=False,inplace=True)
        full_df.reset_index(drop=True,inplace=True)
        full_df.to_json('esports-data/tournaments-cleaned.json', orient="records")

def process_ingame_event(game_event):
    '''
    Each event is processed in a way to keep only the data that we will use.
    The process changes depending on the event itself.
    '''
    processed_output = None
    game_event_type = game_event.get('eventType')

    def convert_team_id(teamID):
        '''I'm a bit paranoid when some events are concerned. For instance,
        Rift Herald might die/despawn on its own by minute 20.
        Blue side is 100, red side is 200. No team = None'''

        if teamID == 200 or teamID == '200':
            return 'red'
        elif teamID == 100 or teamID == 'blue':
            return 'blue'
        #No team demarcation = it evaporated on its own.
        return None

    #General fields to remove after saving the eventType:
    keys_to_remove = ['eventTime','eventType','platformGameId','gameTime',
                        'stageID','sequenceIndex','gameName','playbackID']
    for key in keys_to_remove:
        game_event.pop(key,None)

    #Pick-ban info can sometimes be logged (files are inconsistent). Let's
    #accommodate for those:
    if game_event_type == 'champ_select':

        ban_list = []
        team_one_data = []
        team_two_data = []

        for banned_champion in game_event.get('bannedChampions'):
            ban_data = {
                'team': convert_team_id(banned_champion.get('teamID')),
                'championID': banned_champion.get('championID')
            }
            ban_list.append(ban_data)

        for player in game_event.get('teamOne'):
            player_data = {
                'participantID': player.get('participantID'),
                'summonerName': player.get('summonerName'),
                'championID': player.get('championID')
            }
            team_one_data.append(player_data)

        for player in game_event.get('teamTwo'):
            player_data = {
                'participantID': player.get('participantID'),
                'summonerName': player.get('summonerName'),
                'championID': player.get('championID')
            }
            team_two_data.append(player_data)

        processed_output = {
            'bannedChampions': ban_list,
            'teamOne': team_one_data,
            'teamTwo': team_two_data
        }


    #Initial game info: The format of our output is a list of values:
    #[participantID, summonerName, 'blue' or 'red', 'championName]
    if game_event_type == 'game_info':

        blue_team = []
        red_team = []

        for player in game_event.get('participants',None):
        #Getting each relevant field for every player
            player_data = {
                'participantID': player.get('participantID',None),
                'summonerName': player.get('summonerName',None),
                'championName': player.get('championName',None)
            }

            #Adding it into either list depending on what we're dealing with:
            #Blue is 100, red is 200
            team_side = player.get('teamID',None)
            if team_side == 100 or team_side == '100':
                blue_team.append(player_data)
            elif team_side == 200 or team_side == '200':
                red_team.append(player_data)

        processed_output = {
            'blue': blue_team,
            'red': red_team
        }

    #Turret plates:
    if game_event_type == 'turret_plate_destroyed':
        processed_output = {
            'team': convert_team_id(game_event.get('teamID')),
            'lane': game_event.get('lane')
        }

    #Buildings destroyed: note the NULL on turretTier when dealing with inhibs.
    if game_event_type == 'building_destroyed':
        processed_output = {
            'team': convert_team_id(game_event.get('teamID')),
            'lane': game_event.get('lane'),
            'buildingType': game_event.get('buildingType')
        }
        if game_event.get('turretTier',None):
            processed_output.update({'turretTier': game_event.get('turretTier')})

    #Jungle monster kills:
    if game_event_type == 'epic_monster_kill':
        processed_output = {
            'monsterType': game_event.get('monsterType'),
            'killer': game_event.get('killer'),
            'team': convert_team_id(game_event.get('killerteamID')),
            'inEnemyJungle': game_event.get('inEnemyJungle'),
        }

    #Wards placed
    if game_event_type == 'ward_placed':
        processed_output = {
            'placer': game_event.get('placer'),
            'wardType': game_event.get('wardType'),
            'position': game_event.get('position')
        }

    #Wards killed
    if game_event_type == 'ward_killed':
        processed_output = {
            'killer': game_event.get('killer'),
            'wardType': game_event.get('wardType'),
            'position': game_event.get('position')
        }

    #Deliberate champion kills
    if game_event_type == 'champion_kill':
        processed_output = {
            'killerTeam': convert_team_id(game_event.get('killerTeamID',None)),
            'victimTeam': convert_team_id(game_event.get('victimTeamID',None)),
            'killer': game_event.get('killer'),
            'assistants': game_event.get('assistants'),
            'position': game_event.get('position')
        }

    #Special champion kill alerts: proceed in reverse of what we usually do.
    if game_event_type == 'champion_kill_special':
        processed_output = game_event #Unsure how to process it beyond that

    #Stats updates
    if game_event_type == 'stats_update':

        tracked_stats = ['TOTAL_DAMAGE_DEALT_TO_CHAMPIONS', 'TOTAL_DAMAGE_TAKEN',
                        'TIME_CCING_OTHERS', 'VISION_SCORE',
                        'NEUTRAL_MINIONS_KILLED',
                        'NEUTRAL_MINIONS_KILLED_YOUR_JUNGLE',
                        'NEUTRAL_MINIONS_KILLED_ENEMY_JUNGLE',
                        'MINIONS_KILLED', 'CHAMPIONS_KILLED',
                        'NUM_DEATHS', 'NUM_ASSISTS',
                        'TOTAL_DAMAGE_DEALT_TO_OBJECTIVES']

        info_dump = []

        for participant in game_event.get('participants'):
            #All participants have specific data points that we want to acquire
            participant_data = {
                'participantID': participant.get('participantID'),
                'XP': participant.get('XP'),
                'totalGold': participant.get('totalGold'),
            }

            #We also want specific stats to appear in our data log:
            for stat_category in participant['stats']:

                if stat_category.get('name') in tracked_stats:
                    participant_data.update({stat_category.get('name'):
                                            stat_category.get('value')})

            info_dump.append(participant_data)

        blue_status = None
        red_status = None
        team_status = game_event.get('teams',None)

        for side in team_status:
            indicator = side.pop('teamID')
            if indicator == 100:
                blue_status = side
            elif indicator == 200:
                red_status = side

        #Afterwards, we ship it alongside the teams info (which might be redundant)
        processed_output = {
            'participants':info_dump,
            'blue': blue_status,
            'red': red_status
        }

    #Endgame event contains the winner:
    if game_event_type == 'game_end':
        processed_output = {'winningTeam':
                            convert_team_id(game_event.get('winningTeam',None))}

    #Are we drawing a blank on whatever this event is? (Processing returns None)
    if processed_output is not None:
        return processed_output

    #If we actually are dealing with weird events:
    #(subject to investigation, it will notably show up)
    return game_event

def we_want_to_document_this_event(game_event) -> bool:
    '''
    The name of this function is self-explanatory: we want to filter out unnecessary events.
    We can modify things here in the future.
    '''
    #Too many status updates.
    #Item builds aren't tracked at this time.
    #Skill level-ups aren't tracked at this time.
    event_filter = ['stats_update','item_purchased','item_destroyed',
                    'item_undo','item_sold','skill_level_up','champ_select',
                    'summoner_spell_used']

    #For monsters, there might be a case for tracking camp takedowns, especially if we're
    #looking at experience differences or counter-jungling. (Yes, that is possible)
    #Speculation: Tony might want to see that data later. Please ask him how he wants to
    #see it represented.
    #Update: Tony wants to track these things. I'll keep them in the data acquisition process,
    #but I'll sift through them in the event processing stage.
    epic_monster_filter = ['blueCamp','redCamp','gromp','wolf','krug','raptor',None]

    #No event found: either it's messy data, or there's nothing to record.
    if game_event.get('eventType',None) is None:
        return False

    #Temporary: we don't want to track all jungle camp takedowns. Keep it to Scuttle,
    #Herald, Dragon, and Baron for now. That said, if Tony says you take it, feel free
    #to turn the entire section below into commentary.
    if game_event.get('eventType') == 'epic_monster_kill':
        #For now, I only want to check for Dragons, Herald, and Barons. Unless it's a counterjungling angle.
        #Counterjungle = inEnemyJungle = True, so we'll want to record it.
        #Tony can decide to take all events no matter what for jungle pathing and whatnot in the future
        
        return True #game_event.get('inEnemyJungle')) # or game_event.get('monsterType') not in epic_monster_filter)

    #We can add other levels for this, by the way.
    level_ups_of_interest = [2,6,11,16,18]
    if game_event.get('eventType') == 'champion_level_up':
        return (game_event.get('level',0) in level_ups_of_interest)

    #Tony and I observed that pings on the map sometimes counted as "wards" placed by nobody. Let's take those out.
    if game_event.get('eventType') == 'ward_placed':
        return (game_event.get('placer', 0) != 0)

    #Past the point above, if it isn't any of the events filtered, we take that in.
    return (game_event.get('eventType',None) not in event_filter)

    #Leave commented: this was used to demonstrate how much space we would save by only filtering stats updates.
    #no_stats_update = ['stats_update']
    #return (game_event.get('eventType',None) not in no_stats_update)

def extract_useful_data(game_json):
    '''
    This function receives a json.load() that should not be empty or
    corrupted. When invoking this function, use try: except:
    '''
    event_list = []

    #We're using this to build each entry. Copypasting big blocks of code isn't
    #something I fancy, so here's to simplifying.
    def build_event_dict(my_timestamp,current_event,label=None):

        event_label = label if label is not None else current_event.get('eventType',None)
        processed_event = process_ingame_event(current_event)
        output_dict = {
            'gameTime': my_timestamp,
            'eventType': event_label
        }
        output_dict.update(processed_event)
        return output_dict

    #Which stat updates do we want to record?
    stat_update_obtained = {
        600: False, #10-minute updates
        900: False, #15-minute updates
        'Endgame': False #Final game update
    }

    has_pick_ban_updates = False
    game_info_found = False
    champ_select_info = None

    for game_event in game_json:

        #If we still haven't found 'game_info' as an event, keep pushing the
        #game timer back.
        if not game_info_found:
            initial_timestamp_str = game_event.get('eventTime')
            initial_timestamp = dt.datetime.fromisoformat(initial_timestamp_str.replace('Z','+00:00'))
            platform_id = game_event.get('platformGameId')
            patch_info = game_event.get('gameVersion', None)

        #Get relevant time info for this event
        event_timestamp = game_event.get('eventTime')
        event_timestamp = dt.datetime.fromisoformat(event_timestamp.replace('Z','+00:00'))
        game_timer = (event_timestamp - initial_timestamp) / dt.timedelta(seconds=1)

        has_pick_ban_updates = (game_event.get('eventType') == 'champ_select')

        if has_pick_ban_updates:
            champ_select_info = build_event_dict(game_timer,game_event)

        if game_event.get('eventType',None) == 'game_info':
            game_info_found = True
            event_list.append({
                'gameDate': initial_timestamp_str.replace('Z','+00:00'),
                'esportsPlatformId': platform_id,
                'gameVersion': patch_info
            })

            if champ_select_info is not None:
                event_list.append(champ_select_info)

        if we_want_to_document_this_event(game_event):
            event_list.append(build_event_dict(game_timer,game_event))

        #That said, some specific stats_updates are worth taking and dissecting later.
        elif game_event.get('eventType',None) == 'stats_update':

            #10-minute mark stat update
            if not stat_update_obtained[600] and (game_timer>=600):
                event_list.append(build_event_dict(game_timer,game_event,
                                                    'game_state_10mn'))
                stat_update_obtained[600]=True

            #15-minute mark stat update
            if not stat_update_obtained[900] and (game_timer>=900):
                event_list.append(build_event_dict(game_timer,game_event,
                                                    'game_state_15mn'))
                stat_update_obtained[900]=True

            #Endgame stat update
            if not stat_update_obtained['Endgame'] and game_event.get('gameOver',False):
                event_list.append(build_event_dict(game_timer,game_event,
                                                    'game_state_end'))
                stat_update_obtained['Endgame']=True

    with open(f"games/{platform_id.replace(':','_')}-cleaned.json",'w') as revamped_file:
        json.dump(event_list,revamped_file)

def prepare_data_for_transformation(year=None):
    '''
    Tweaking Riot's download/data acquisition script to account for a person's
    wish to download all the data for examination.
    '''
    start_time = time.time()
    with open("esports-data/tournaments-cleaned.json", "r") as json_file:
       tournaments_data = json.load(json_file)
    with open("esports-data/mapping_data.json", "r") as json_file:
       mappings_data = json.load(json_file)

    directory = "games"
    if not os.path.exists(directory):
       os.makedirs(directory)

    mappings = {
       esports_game["esportsGameId"]: esports_game for esports_game in mappings_data
    }

    game_counter = 0

    #This is where it goes a bit bleh
    for tournament in tournaments_data:
        start_date = tournament.get("startDate", "")
        #Tweaking Riot's code here: I might want to simply download the entire dataset.
        correct_year = start_date.startswith(str(year)) if year is not None else True

        if correct_year:
            print(f"Processing {tournament['slug']}")
            for stage in tournament["stages"]:
                for section in stage["sections"]:
                    for match in section["matches"]:
                        for game in match["games"]:
                            if game["state"] == "completed":
                                try:
                                    platform_game_id = mappings[game["id"]]["platformGameId"]
                                except KeyError:
                                    print(f"{platform_game_id} {game['id']} not found in the mapping table")
                                    continue

                                game_filename = platform_game_id.replace(':','_')

                                #If a cleaned version of the file doesn't exist:
                                if not os.path.isfile(f"{directory}/{game_filename}-cleaned.json"):

                                    #Download
                                    download_gzip_and_write_to_json(f"{directory}/{platform_game_id}")

                                    #Extract the data, keeping only the important bits
                                    with open(f"{directory}/{game_filename}.json",'r') as game_file:
                                        game_data = json.load(game_file)
                                        extract_useful_data(game_data)

                                    #Delete the source file
                                    os.remove(f"{directory}/{game_filename}.json")

                                game_counter += 1

                            if game_counter % 10 == 0:
                                print(
                                    f"----- Processed {game_counter} games, current run time: \
                                    {round((time.time() - start_time)/60, 2)} minutes"
                                )

def get_missing_lpl_games():
    '''
    The LPL did things differently, and their data was recorded differently
    before the 2023 summer split. For now, let's at least get the data.
    '''
    start_time = time.time()

    directory = "games"
    if not os.path.exists(directory):
       os.makedirs(directory)

    with open("esports-data/mapping_data.json", "r") as json_file:
       mappings_data = json.load(json_file)

    # After extensive notebook usage, we found out that the tournament realms
    # contained LPL for LPL games that weren't documented. That was a huge
    # relief.
    mappings_missing_lpl = {
       esports_game["esportsGameId"]: esports_game for esports_game in mappings_data if 'LPL' in esports_game['platformGameId']
    }

    game_counter = 0

    for game in mappings_missing_lpl.values():

        platform_game_id = game.get('platformGameId')

        game_filename = platform_game_id.replace(':','_')

        #If a cleaned version of the file doesn't exist:
        if not os.path.isfile(f"{directory}/{game_filename}-cleaned.json"):

            #Download
            download_gzip_and_write_to_json(f"{directory}/{platform_game_id}")

            #Extract the data, keeping only the important bits
            with open(f"{directory}/{game_filename}.json",'r') as game_file:
                game_data = json.load(game_file)
                extract_useful_data(game_data)

            #Delete the source file
            os.remove(f"{directory}/{game_filename}.json")

            game_counter += 1

            if game_counter % 10 == 0 and game_counter > 0:
                print(
                    f"----- Processed {game_counter} games, current run time: \
                    {round((time.time() - start_time)/60, 2)} minutes"
                )

#If we want to run the script as a standalone, we can have a go.
if __name__  == '__main__':
    download_esports_files()
    filter_leagues()
    filter_tournaments()
    prepare_data_for_transformation()
    #get_missing_lpl_games()
