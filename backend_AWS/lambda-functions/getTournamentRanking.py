
import boto3
import json
import time
import os

athena = boto3.client('athena')
output_s3= os.getenv('S3_OUTPUT_LOC')
database= os.getenv('DATABASE')
table = os.getenv('TABLE')

def lambda_handler(event, context):
    leagueid = int(event['pathParameters']['tournament_id'])
    if 'stage_name' in event['queryStringParameters']:
        stage_name = event['queryStringParameters']['stage_name']
    else:
        stage_name = None

    leaguelabel=""

    query = f"""
    SELECT t1.team_id, t1.team_code, t1.team_name, t1.rating,t1.leaguelabel
    FROM "all_years_riot"."riot_all_years" t1
    JOIN (
        SELECT team_id, MAX(game_date) AS latest_date
        FROM "all_years_riot"."riot_all_years"
        WHERE leagueid = {leagueid}
          AND (stage_name = '{stage_name}' OR '{stage_name}' IS NULL)
        GROUP BY team_id
    ) t2 ON t1.team_id = t2.team_id AND t1.game_date = t2.latest_date
    ORDER BY t1.rating DESC;
"""


    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'all_years_riot'  
        },
        ResultConfiguration={
            'OutputLocation': output_s3
        }

    )

    query_execution_id = response['QueryExecutionId']
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = response['QueryExecution']['Status']['State']
        if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(3)  # Wait for 5 seconds before checking again

    # If the query succeeded, fetch and process the results
    if query_state == 'SUCCEEDED':
        results = athena.get_query_results(QueryExecutionId=query_execution_id)

        # Process the query results into a JSON response
        rows = results['ResultSet']['Rows'][1:]  # Skip the header row
        team_rankings = []

        for row in rows:
            columns = row['Data']
            leaguelabel=columns[4]['VarCharValue']
            team_rankings.append({
                'team_id': columns[0]['VarCharValue'],
                'team_code':columns[1]['VarCharValue'],
                'team_name': columns[2]['VarCharValue'],
                # 'ranking_points': columns[2]['VarCharValue']
                'ranking_points': columns[3].get('VarCharValue', 'N/A')

            })


    return {
        'statusCode': 200,
        'body': json.dumps({
            'tournament_id': leagueid,
            'stage_name': stage_name,
            'leaguelabel':leaguelabel,
            'teams': team_rankings
        })
    }

