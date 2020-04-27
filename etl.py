"""etl.py is the reads the data files and loads into the Redshit DB hosted in AWS"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Arguments: cursor and filepath
Return value: none"""
    """ 
    Summary line. 
  
    load_staging_tables function reads the Json data from S3 and loads it into the staging tables in Redshift db
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (string): Contains the connection information 
  
    Returns: 
    None
  
    """
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
Arguments: cursor and filepath
Return value: none"""
    """ 
    Summary line. 
  
    insert_tables function reads the data from the staging tables and inserts it into the final Reshift tables 
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (string): Contains the connection information 
  
    Returns: 
    None
  
    """
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    """ 
    Summary line. 
  
    main control the flow, it establishes the connection with Redshift DB sets the cursor and call the functions to load the staging tables and then to the final tables.
  
    Parameters: 
    None
  
    Returns: 
    None 
  
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()