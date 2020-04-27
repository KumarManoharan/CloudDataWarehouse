"""create_tables.py  drops the tables(in case of a rerun) and creates the tables required in Redshift"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

    """ 
    Summary line. 
  
    drop_tables function loops through the list of tables from sql_queries.py file and drops them one by one
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (string): Contains the connection information 
  
    Returns: 
    None
  
    """
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    """ 
    Summary line. 
  
    create_tables function reads the table creation scripts from sql_queries.py file and creates the tables in the dwh redshit db
    
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (string): Contains the connection information 
  
    Returns: 
    None
  
    """
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


    """ 
    Summary line. 
  
    main function control the flow, it establishes the connection with Redshit, drops and creates the tables.
  
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()