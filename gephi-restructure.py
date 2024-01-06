import psycopg2
import pandas as pd
import csv
import sys
import os
from psycopg2.extensions import connection
from typing import List, Tuple, Dict, Optional
import logging

# Logging configuration
logging.basicConfig(filename='logs/runtime_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL config
ConfigDict = Dict[str, str]

# Change the config values accordingly
db_params: ConfigDict = {
    'dbname': 'your_dbname',
    'user': 'your_username',
    'password': 'your_dbpass',
    'host': 'your_host'
}

def db_connect(db_params: ConfigDict) -> Optional[connection]:
    '''
    Establish a connection to the PostgreSQL db.
    '''
    try:
        conn: connection = psycopg2.connect(**db_params)
        logging.info('Database connection established.')
        return conn
    except psycopg2.Error as err:
        logging.error(f'Error connecting to the PostgreSQL database: {err}')
        return None

def db_pull(conn: connection) -> Optional[pd.DataFrame]:
    '''
    Pull the data from the database.
    '''

    # NOTE: Need to add where clause to fetch new records
    sql_query: str = """
        SELECT q.question as Question,
               q.answer as Answer,
               q.qaLabel as Label,
               s.value as SubTopic,
               t.value as Topic,
               m.value as MacroTopic
        FROM ArticleGeneratedQna q
        JOIN QnaSubtopic s ON q.QnaSubtopicId = s.Id
        JOIN Topic t ON s.TopicId = t.Id
        JOIN Macrotopic m ON t.MacrotopicId = m.Id;
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

def db_push():
    pass

def gephi_restructure(df: pd.DataFrame):
    pass

def main():
    pass

if __name__ == '__main__':
    main()
