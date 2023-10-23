import boto3
import json
import uuid
import time
import os

athena = boto3.client('athena')
output_s3= os.getenv('S3_OUTPUT_LOC')
year= os.getenv('YEAR')
database= os.getenv('DATABASE')
table = os.getenv('TABLE')

def lambda_handler(event, context):
    client_request_token = str(uuid.uuid4())

    number_of_teams = int(event['queryStringParameters'].get('number_of_teams', 20))  # Default to 20 teams
    
    
    query= f"""
    WITH ranked_teams AS (
    SELECT team_id, team_code, team_name, rating, game_date,
           RANK() OVER (PARTITION BY team_id ORDER BY game_date DESC) AS rank
    FROM "{database}"."{table}")
    SELECT team_id, team_code, team_name, rating
    FROM ranked_teams
    WHERE rank = 1
    ORDER BY rating DESC
    LIMIT {number_of_teams}
    """

    # Run the Athena query
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_s3
        }
    )

    query_execution_id = response['QueryExecutionId']
    print("query_execution_id",query_execution_id)
    
    team_rankings = []

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
        print("results",rows)
        # team_rankings = []

        for row in rows:
            columns = row['Data']
            team_rankings.append({
                'team_id': columns[0]['VarCharValue'],
                'team_code':columns[1]['VarCharValue'],
                'team_name': columns[2]['VarCharValue'],
                'ranking_points': columns[3].get('VarCharValue', 'N/A')

            })
            
    headers = {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'OPTIONS,GET' 
    }

    return {
        'statusCode': 200,
        'headers': headers,
        'body': json.dumps({
        'teams': team_rankings
    })
        }

