"""create_tables.py

This script connects to the Amazon Redshift cluster and create tables for the cluster
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops existing tables by running drop_table_queries in sql_queries.py
    
    Parameters
    ----------
    cur: cursor
        The cursor object
    conn: connection
        The connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates tables by running create_table_queries in sql_queries.py
    
    Parameters
    ----------
    cur: cursor
        The cursor object
    conn: connection
        The connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Amazon Redshift cluster using credentials in dwh.cfg
    
    Calls drop_tables to drop existing tables
    
    Calls create_tables to create new tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()