import os
import json
from typing import List, Tuple, Dict, Optional
import logging

import psycopg2
import boto3

from utils.gephi_restructure import get_db_version, db_connect, db_pull, db_push, gephi_restructure, db_rollback, get_db_tables

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
        print(f'PostgreSQL version: {db_version}')

        db_tables = get_db_tables(conn)
        print(f'Tables in Postgres database: {db_tables}')

        df_data = db_pull(conn)

        if df_data is not None and not df_data.empty:
            print(f'Found {len(df_data)} records matching the query.')
            print(f'Database is not empty. . .{df_data.to_dict()}')
            
          
            df_nodes, df_edges = gephi_restructure(df_data)
            
            print(f'Restructured the data')
            print(f'Nodes table Length: {len(df_nodes)}')
            print(f'Nodes table sample data: {df_nodes.head(1).to_dict()}')
            print(f'Edges table length: {len(df_edges)}')
            print(f'Edges table sample data: {df_edges.head(1).to_dict()}')

            # Pushing the data backl to the database 
            db_push_status = db_push(conn, df_nodes, df_edges, df_data)

            if db_push_status:    
                # Close the connection
                conn.close()
                print('Closing the database connection. . .')
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'successfully restructured the data!'})
                }
            else:
                # Rollback the changes
                db_rollback(conn)
                print('Failed to push data, rolling back changes. . .')

                # Close the conneciton
                conn.close()
                print('Closing the database connection. . .')

                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Restructuring failed. . .'})
                } 
        else:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Restructuring failed. . .No data exists in database.'})
            } 
    else:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Restructuring failed. . .connection to database failed.'})
        } 