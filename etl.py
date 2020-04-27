"""etl.py

This script connects to the Amazon Redshift cluster and runs the ETL pipeline to insert records into tables
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data into staging tables by running copy_table_queries in sql_queries.py
    
    Parameters
    ----------
    cur: cursor
        The cursor object
    conn: connection
        The connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data into star schema tables by running insert_table_queries in sql_queries.py
    
    Parameters
    ----------
    cur: cursor
        The cursor object
    conn: connection
        The connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Amazon Redshift cluster using credentials in dwh.cfg
    
    Calls load_staging_tables to load data into the staging tables
    
    Calls insert_tables to insert data into star schema tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()