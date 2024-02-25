import os
import json
from typing import List, Tuple, Dict, Optional
import logging

# Logging configuration
logging.basicConfig(filename='gephi_restructure.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import psycopg2
import boto3

from utils.gephi_restructure import db_connect, db_pull, db_push, gephi_restructure

# PostgreSQL config
ConfigDict = Dict[str, str]

db_params: ConfigDict = {
    'DBNAME': 'pdf2qadev',
    'USER': 'dev_user',
    'ENDPOINT': 'pdf2qa-dev20231229171910310300000001.clfojyqicnb4.eu-west-2.rds.amazonaws.com',
    'REGION': 'eu-west-2',
    'PORT': '5432'
}

def main():
    # Establish the connection to the database
    conn = db_connect(db_params)

    if conn:
        df_data = db_pull(conn)

        if df_data is not None and not df_data.empty:
            df_nodes, df_edges = gephi_restructure(df_data)

            # Pushing the data backl to the database 
            db_push(conn, df_nodes, df_edges)
    
    # Close the connection
    conn.close()
    logging.info('Closing the database connection. . .')


def lambda_handler():
    # TODO implement

    main()
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'successfully restructured the data!'})
    }

lambda_handler()