import boto3
import json
import time
import os

output_s3= os.getenv('S3_OUTPUT_LOC')
database= os.getenv('DATABASE')
table= os.getenv('TABLE')

athena = boto3.client('athena')

def lambda_handler(event, context):
    
    tournament_ids = [int(tid.strip('[]').strip()) for tid in event['queryStringParameters'].get('tournament_id', '').split(',') if tid]
    team_ids = [int(tid.strip('[]').strip()) for tid in event['queryStringParameters'].get('team_id', '').split(',') if tid]


    # Construct the Athena query based on tournament_ids and team_ids
    query = construct_athena_query(tournament_ids, team_ids)

    # Execute the Athena query
    query_execution_id = execute_athena_query(query)

    # Get the query results
    results = get_athena_query_results(query_execution_id)

    # Process the query results into the desired structure
    processed_data = process_athena_results(results)

    # Return the processed data as JSON
    response_data = {
        "teamRanking": processed_data
    }
    headers = {
            'Access-Control-Allow-Origin': '*', 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,GET' 
        }


    return {
        'statusCode': 200,
        'headers': headers,
        'body': json.dumps(response_data)
    }

def construct_athena_query(tournament_ids, team_ids):
    
    # Construct the Athena SQL query
    query = """
    SELECT tournament_name, team_id, team_code,team_name, rating, rank_team
    FROM "team_rankings"."team_rankings"
    WHERE leagueid IN ({tournament_ids}) 
    """.format(tournament_ids=",".join([str(tid) for tid in tournament_ids]))

    if team_ids:
        query += "AND team_id IN ({team_ids})".format(team_ids=",".join([str(tid) for tid in team_ids]))
        
    query+= "ORDER BY leagueid ASC, rank_team ASC"

    return query


def execute_athena_query(query):
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_s3
        }
    )
    print("response",response['QueryExecutionId'])
    return response['QueryExecutionId']

def get_athena_query_results(query_execution_id):
    
    
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = response['QueryExecution']['Status']['State']
        if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5) 
        
        
    # If the query succeeded, fetch and process the results
    if query_state == 'SUCCEEDED':
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        
    return results



def process_athena_results(results):
    rows = results['ResultSet']['Rows'][1:]  # Skip the header row
    tournament_data = {}

    for row in rows:
        columns = row['Data']
        tournament_id = columns[0]['VarCharValue']
        team_id = columns[1]['VarCharValue']
        team_code = columns[2]['VarCharValue']
        team_name = columns[3]['VarCharValue']
        ranking_points = float(columns[4]['VarCharValue'])  # Convert to float
        rank = int(columns[5]['VarCharValue'])

        # Create a team ranking dictionary
        team_ranking = {
            "team_id": team_id,
            "team_code": team_code,
            "team_name": team_name,
            "ranking_points": ranking_points,
            "rank": rank
        }

        # If the tournament_id is not in the dictionary, create an entry
        if tournament_id not in tournament_data:
            tournament_data[tournament_id] = {"tournament_id": tournament_id, "team_rankings": []}

        # Append the team ranking to the tournament data
        tournament_data[tournament_id]["team_rankings"].append(team_ranking)

    # Extract the values from the dictionary and add to the result list
    processed_data = list(tournament_data.values())

    return processed_data


