import json
from utils.gephi_restructure import main


def lambda_handler(event, context):
    # TODO implement

    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Gephi restructure script. . .')
    }