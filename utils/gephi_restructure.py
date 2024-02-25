import psycopg2
import sys
import os
import boto3
import pandas as pd
from psycopg2.extensions import connection
from typing import List, Tuple, Dict, Optional
import logging

# Logging configuration
logging.basicConfig(filename='./logs/gephi_restructure.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL config
ConfigDict = Dict[str, str]

def db_connect(db_params: ConfigDict) -> Optional[connection]:
    '''
    Establish a connection to the PostgreSQL db.
    '''
    try:
        client = boto3.client('rds',endpoint_url=db_params['ENDPOINT'],region_name=db_params['REGION'])

        #session = boto3.Session(profile_name='RDSCredsTestProfile')
        #session = boto3.Session(
        #    aws_access_key_id='',
        #    aws_secret_access_key='',
        #    region_name=''
        #    )

        token = client.generate_db_auth_token(DBHostname=db_params['ENDPOINT'], 
                                            Port=db_params['PORT'], 
                                            DBUsername=db_params['USER'], 
                                            Region=db_params['REGION'])
        logging.info('Boto session created.')
        #db_params['token'] = token
    except Exception as err:
        logging.info('Something went wrong. . .', err)
        return None

    try:
        conn: connection = psycopg2.connect(host=db_params['ENDPOINT'], port=db_params['PORT'], database=db_params['DBNAME'], user=db_params['USER'], password=token)
        logging.info('Database connection established.')
        return conn
    except psycopg2.Error as err:
        logging.error(f'Error connecting to the PostgreSQL database: {err}')
        return None


def db_pull(conn: connection) -> Optional[pd.DataFrame]:
    '''
    Pull the data from the database.
    '''

    # TODO: Need to add where clause to fetch new records
    sql_query: str = """
            SELECT
                s.name as SubTopic,
                t.name as Topic,
                m.name as MacroTopic,
            FROM
                qnaSubtopic s
            JOIN
                Topic t ON s.topicid = t.id
            JOIN
                Macrotopic m ON t.macrotopicid = m.id
            WHERE
                
        """

    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=[desc for desc in cur.description()])
            logging.info('Data fetched successfully.')
            return df
    except psycopg2.Error as err:
        logging.error(f'Error fetching the data from the PostgreSQL database: {err}')
        return None
    
    
def print_results(results: List[Tuple]) -> None:
    '''
    Log the results fetched from the database.
    '''
    for row in results:
        logging.info(f"Row: {row}")


def db_push(conn: connection, df_nodes: pd.DataFrame, df_edges: pd.DataFrame) -> bool:
    '''
    Push nodes and edges DataFrames to the GephiNode and GephiEdges tables in the database.
    '''
    try:
        with conn.cursor() as cur:
            # Push nodes to GephiNode table
            for _, row in df_nodes.iterrows():
                cur.execute("INSERT INTO GephiNode (nodeLabel) VALUES (%s);", (row['nodeLabel'],))

            # Push edges to GephiEdges table
            for _, row in df_edges.iterrows():
                cur.execute("INSERT INTO GephiEdges (source, target) VALUES (%s, %s);", (row['Source'], row['Target']))

            # Commit the changes
            conn.commit()

            logging.info('Data pushed successfully.')
            return True
    except psycopg2.Error as err:
        logging.error(f'Error pushing data to the database: {err}')
        return False
 

def gephi_restructure(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Restructure the data for Gephi and return nodes and edges DataFrames.
    '''
    # Create a unique set of nodes from topics, subtopics, and macrotopics
    unique_nodes = set(df['SubTopic']) | set(df['Topic']) | set(df['MacroTopic'])

    # Create a DataFrame for nodes
    nodes_df = pd.DataFrame(list(unique_nodes), columns=['nodeLabel'])

    # Create a DataFrame for edges
    edges_df = pd.DataFrame(columns=['Source', 'Target', 'Type'])

    # Iterate through the DataFrame to create edges
    for index, row in df.iterrows():
        source_node = row['MacroTopic']
        target_node = row['Topic']
        edges_df = edges_df.append({'Source': source_node, 'Target': target_node, 'Type': 'undirected'}, ignore_index=True)

        source_node = row['Topic']
        target_node = row['SubTopic']
        edges_df = edges_df.append({'Source': source_node, 'Target': target_node, 'Type': 'undirected'}, ignore_index=True)

    return nodes_df, edges_df
