import sys
import os
from typing import List, Tuple, Dict, Optional
import logging

import boto3
import psycopg2
from psycopg2.extensions import connection

import pandas as pd


# PostgreSQL config
ConfigDict = Dict[str, str]

def get_db_tables(conn: connection) -> List[str]:
    sql_query = """
    SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';
    """
    try:
        with conn.cursor() as curr:
            curr.execute(sql_query)
            result = curr.fetchall()

            table_names = [row[0]  for row in result]

            return table_names
    except psycopg2.Error as err:
        print(f'Error fetching the data from the PostgreSQL database: {err}')
        return None


def get_db_version(conn: connection)-> List[str]:
        sql_query = "SELECT version();"
        try:
            with conn.cursor() as curr:
                curr.execute(sql_query)
                result = curr.fetchone()

                return result[0]
        except psycopg2.Error as err:
            print(f'Error fetching the data from the PostgreSQL database: {err}')
            return None


def db_connect(db_params: ConfigDict) -> Optional[connection]:
    '''
    Establish a connection to the PostgreSQL db.
    '''
    try:
        conn: connection = psycopg2.connect(
            host=db_params['ENDPOINT'], 
            port=db_params['PORT'], 
            database=db_params['DBNAME'], 
            user=db_params['USER'], 
            password=db_params['PASSWORD'])
        
        print('Database connection established.')
        return conn
    
    except psycopg2.Error as err:
        print(f'Error connecting to the PostgreSQL database: {err}')
        return None


def db_pull(conn: connection) -> Optional[pd.DataFrame]:
    '''
    Pull the data from the database.
    '''
    sql_query: str = """
            SELECT
                s.id as SubTopicId,
                s.name as SubTopic,
                t.id as TopicId,
                t.name as Topic,
                m.id as MacroTopicId,
                m.name as MacroTopic
            FROM
                qnaSubtopic s
            JOIN
                Topic t ON s.topicid = t.id
            JOIN
                Macrotopic m ON t.macrotopicid = m.id
            WHERE
                (s.Status IS NULL OR s.Status = 0) 
                AND (t.Status IS NULL OR t.Status = 0)
                AND (m.Status IS NULL OR m.Status = 0);
        """

    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            data = [dict(zip(columns, row)) for row in rows]
            df: pd.DataFrame = pd.DataFrame(data)
            if df.shape[0] > 0:
                print('Data fetched successfully.')
            else:
                print('No data in the table.')
            return df
    except psycopg2.Error as err:
        print(f'Error fetching the data from the PostgreSQL database: {err}')
        return None
   

def db_push(conn: connection, df_nodes: pd.DataFrame, df_edges: pd.DataFrame, df: pd.DataFrame) -> bool:
    '''
    Push nodes and edges DataFrames to the GephiNode and GephiEdges tables in the database.
    '''
    try:
        with conn.cursor() as cur:
            # Push nodes to GephiNode table
            for _, row in df_nodes.iterrows():
                cur.execute("""INSERT INTO GephiNode (id, nodeLabel, subtopicid, topicid, macrotopicid) VALUES (%s, %s, %s, %s, %s);""", 
                            (row['id'], row['nodeLabel'], row['subtopicid'], row['topicid'], row['macrotopicid']))

            # Push edges to GephiEdges table
            for _, row in df_edges.iterrows():
                cur.execute("INSERT INTO GephiEdges (id, sourceId, source, targetId, target, type) VALUES (%s, %s, %s, %s, %s, %s);", 
                           (row['id'], row['SourceId'], row['Source'], row['TargetId'], row['Target'], row['Type']))

            # Update the Status field in qnaSubtopic, Macrotopic, and Topic tables
            subtopic_ids = df_nodes.loc[df_nodes['nodeLabel'].isin(df['subtopic']), 'id'].tolist()
            topic_ids = df_nodes.loc[df_nodes['nodeLabel'].isin(df['topic']), 'id'].tolist()
            macrotopic_ids = df_nodes.loc[df_nodes['nodeLabel'].isin(df['macrotopic']), 'id'].tolist()

            cur.execute("UPDATE qnaSubtopic SET Status = 1 WHERE id IN %s;", (tuple(subtopic_ids),))
            cur.execute("UPDATE Topic SET Status = 1 WHERE id IN %s;", (tuple(topic_ids),))
            cur.execute("UPDATE Macrotopic SET Status = 1 WHERE id IN %s;", (tuple(macrotopic_ids),))

            # Commit the changes
            conn.commit()

            logging.info('Data pushed successfully.')
            return True
    except psycopg2.Error as err:
        logging.error(f'Error pushing data to the database: {err}')
        return False




def db_rollback(conn: connection):
    try:
        conn.rollback()
    except psycopg2.Error as err:
        print(f'Error rolling back PostgreSQL database: {err}')
 

def gephi_restructure(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Restructure the data for Gephi and return nodes and edges DataFrames.
    '''
    # Create a unique set of nodes from topics, subtopics, and macrotopics
    unique_nodes = set(df['subtopic']) | set(df['topic']) | set(df['macrotopic'])

    # Create the gephiNodes DataFrame
    gephiNodes = pd.DataFrame(list(unique_nodes), columns=['nodeLabel'])
    gephiNodes['id'] = gephiNodes.index + 1  # Assign unique ids starting from 1

    # Assign subtopicid, topicid, and macrotopicid based on node type
    gephiNodes['subtopicid'] = gephiNodes['nodeLabel'].apply(lambda x: df.loc[df['subtopic'] == x, 'subtopicid'].values[0] if x in df['subtopic'].values else None)
    gephiNodes['topicid'] = gephiNodes['nodeLabel'].apply(lambda x: df.loc[df['topic'] == x, 'topicid'].values[0] if x in df['topic'].values else None)
    gephiNodes['macrotopicid'] = gephiNodes['nodeLabel'].apply(lambda x: df.loc[df['macrotopic'] == x, 'macrotopicid'].values[0] if x in df['macrotopic'].values else None)

    # Create the gephiEdges DataFrame
    gephiEdges = pd.DataFrame(columns=['id', 'sourceid', 'source', 'targetid', 'target', 'type'])

    # Function to add edge if it doesn't already exist
    def add_edge(source_node, target_node, gephiEdges):
        source_id = gephiNodes.loc[gephiNodes['nodeLabel'] == source_node, 'id'].values[0]
        target_id = gephiNodes.loc[gephiNodes['nodeLabel'] == target_node, 'id'].values[0]
        
        # Check if the edge already exists
        if not ((gephiEdges['sourceid'] == source_id) & (gephiEdges['targetid'] == target_id) | 
                (gephiEdges['sourceid'] == target_id) & (gephiEdges['targetid'] == source_id)).any():
            new_id = gephiEdges['id'].max() + 1 if not gephiEdges.empty else 1
            new_edge = pd.DataFrame({'id': [new_id],
                                    'sourceid': [source_id],
                                    'source': [source_node],
                                    'targetid': [target_id],
                                    'target': [target_node],
                                    'type': ['undirected']})
            gephiEdges = pd.concat([gephiEdges, new_edge], ignore_index=True)
        return gephiEdges

    # Iterate through the DataFrame to create edges
    for index, row in df.iterrows():
        gephiEdges = add_edge(row['macrotopic'], row['topic'], gephiEdges)
        gephiEdges = add_edge(row['topic'], row['subtopic'], gephiEdges)

    # Reset the index of gephiEdges to ensure continuous indexing
    gephiEdges.reset_index(drop=True, inplace=True)
    
    return gephiNodes, gephiEdges