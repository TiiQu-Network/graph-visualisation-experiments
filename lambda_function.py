import os
import json
from typing import List, Tuple, Dict, Optional
import logging

# Logging configuration
logging.basicConfig(filename='./logs/gephi_restructure.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import psycopg2
import boto3

from utils.gephi_restructure import get_db_version, db_connect, db_pull, db_push, gephi_restructure, db_rollback

# PostgreSQL config
ConfigDict = Dict[str, str]

db_params: ConfigDict = {
    'DBNAME': os.environ['DBNAME'],
    'USER': os.environ['USER'],
    'ENDPOINT': os.environ['ENDPOINT'],
    'REGION': os.environ['REGION'],
    'PORT': os.environ['PORT'],
    'PASSWORD': os.environ['PASSWORD']
}


def lambda_handler(event, context):
    conn = db_connect(db_params)

    if conn:
        db_version = get_db_version(conn)
        print(f"PostgreSQL version: {db_version}")
        df_data = db_pull(conn)

        if df_data is not None and not df_data.empty:
            df_nodes, df_edges = gephi_restructure(df_data)

            # Pushing the data backl to the database 
            db_push_status = db_push(conn, df_nodes, df_edges)

            if db_push_status:    
                # Close the connection
                conn.close()
                logging.info('Closing the database connection. . .')
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'successfully restructured the data!'})
                }
            else:
                # Rollback the changes
                db_rollback()
                logging.info('Failed to push data, rolling back changes. . .')

                # Close the conneciton
                conn.close()
                logging.info('Closing the database connection. . .')

                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Restructuring failed. . .'})
                } 