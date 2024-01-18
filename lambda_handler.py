import json
from utils.gephi_restructure import db_connect, db_pull, db_push

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Gephi restructure script. . .')
    }