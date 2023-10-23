# Complete, integrated script for generating the team report

import pandas as pd
import json


def get_team_matches_updated(team_id, mapping_data):
    """Return matches for a given team_id, including the esportsPlatformId and the side (blue or red)"""
    matches = []
    for entry in mapping_data:
        team_mapping = entry.get('teamMapping', {})
        if team_mapping.get('100') == team_id:
            matches.append({
                "esportsPlatformId": entry['platformGameId'],
                "side": "blue"
            })
        elif team_mapping.get('200') == team_id:
            matches.append({
                "esportsPlatformId": entry['platformGameId'],
                "side": "red"
            })
    return matches


def compute_total_game_lp_updated(data_path, esportsPlatformId, variable_weights):
    """Computes LP for a given game based on various variable weights, with error handling"""
    def calculate_crush(row):
        version = row['version']
        duration = row['gameDuration']
        if duration <= quantiles.loc[version, 0.25]:
            return 1.25
        elif duration <= quantiles.loc[version, 0.75]:
            return 1
        else:
            return 1.25

    data = pd.read_csv(data_path)
    data['version'] = data['gameVersion'].str.split('.').str[0:2].str.join('.')
    data = data.dropna(subset=['version'])
    quantiles = data.groupby('version')['gameDuration'].quantile(
        [0.25, 0.75]).unstack()
    data['crush'] = data.apply(calculate_crush, axis=1)
    side_values = data.groupby(
        ['version', 'winner']).size().unstack().fillna(0)
    side_values['side'] = side_values['blue'] - side_values['red']
    data['side'] = data['version'].map(side_values['side'])

    filtered_data = data[data['esportsPlatformId'] == esportsPlatformId]
    if filtered_data.empty:
        return {"Blue": 0, "Red": 0}
    game_data = filtered_data.iloc[0]
    lp_points = {"Blue": 0, "Red": 0}

    for position in ["Top", "Jg", "Mid", "AD", "Bot"]:
        gold_diff_15 = game_data[f"GoldDiff15{position}"]
        gold_diff_end = game_data[f"GoldDiffEnd{position}"]
        lp_points["Blue"] += variable_weights["GoldDiff"] * \
            (gold_diff_15 + gold_diff_end)
        lp_points["Red"] -= variable_weights["GoldDiff"] * \
            (gold_diff_15 + gold_diff_end)

    game_duration = game_data["gameDuration"]
    lp_points["Blue"] += variable_weights["GameDuration"] * game_duration
    lp_points["Red"] += variable_weights["GameDuration"] * game_duration

    vision_score_blue = sum(game_data[f"VisionScore{position}Blue"] for position in [
                            "Top", "Jg", "Mid", "AD", "Sup"])
    vision_score_red = sum(game_data[f"VisionScore{position}Red"] for position in [
                           "Top", "Jg", "Mid", "AD", "Sup"])
    lp_points["Blue"] += variable_weights["VisionScore"] * vision_score_blue
    lp_points["Red"] += variable_weights["VisionScore"] * vision_score_red

    for team in ["Blue", "Red"]:
        assists_15 = game_data[f"{team}Assists15"]
        deaths_15 = game_data[f"{team}Deaths15"] if game_data[f"{team}Deaths15"] != 0 else 1
        assists_end = game_data[f"{team}AssistsEnd"]
        deaths_end = game_data[f"{team}DeathsEnd"] if game_data[f"{team}DeathsEnd"] != 0 else 1
        lp_points[team] += variable_weights["KillsDeaths"] * \
            ((assists_15 / deaths_15) + (assists_end / deaths_end))

    objectives = {
        "TowerKillsEnd": game_data["BlueTowerKillsEnd"] - game_data["RedTowerKillsEnd"],
        "InhibKillsEnd": game_data["BlueInhibKillsEnd"] - game_data["RedInhibKillsEnd"],
        "BaronKillsEnd": game_data["BlueBaronKillsEnd"] - game_data["RedBaronKillsEnd"],
        "DragonKillsEnd": game_data["BlueDragonKillsEnd"] - game_data["RedDragonKillsEnd"],
        "RiftHeralds": game_data["NbRiftHeraldsBlue"] - game_data["NbRiftHeraldsRed"]
    }
    for objective, value in objectives.items():
        lp_points["Blue"] += variable_weights["Objectives"] * value
        lp_points["Red"] -= variable_weights["Objectives"] * value

    lp_points["Blue"] = lp_points["Blue"] * \
        game_data['crush'] - game_data['side']
    lp_points["Red"] = lp_points["Red"] * \
        game_data['crush'] + game_data['side']

    return lp_points


def generate_team_report_updated(data_csv_path, mapping_json_path, team_id, output_excel_path):
    """Generates a team report based on provided data files and team ID, with error handling"""
    with open(mapping_json_path, 'r', encoding='utf-8') as file:
        mapping_data = json.load(file)
    team_matches = get_team_matches_updated(team_id, mapping_data)
    results = []
    for match in team_matches:
        esportsPlatformId = match['esportsPlatformId']
        lp_points = compute_total_game_lp_updated(
            data_csv_path, esportsPlatformId, variable_weights)
        lp_change = lp_points['Blue'] if match['side'] == 'blue' else lp_points['Red']
        results.append({
            "esportsPlatformId": esportsPlatformId,
            "side": match['side'],
            "lp_change": lp_change
        })
    df_results = pd.DataFrame(results)
    df_results.to_excel(output_excel_path, index=False)
    return f"Report generated and saved to {output_excel_path}"


# Variables
variable_weights = {
    "GoldDiff": 0.1,
    "GameDuration": 0.01,
    "VisionScore": 0.02,
    "KillsDeaths": 1.0,
    "Objectives": 0.5
}

# Uncomment the line below and replace the paths accordingly when you run the script
generate_team_report_updated("path_to_csv_file.csv", "path_to_json_file.json",
                             "team_id_here", "path_to_output_excel_file.xlsx")
# Note: Please replace the file paths and team_id with appropriate values before running the script.
