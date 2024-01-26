import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.

    Parameters:
    cur: Database cursor object.
    conn: Database connection object.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error in drop_tables: {e}")
            conn.rollback()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.

    Parameters:
    cur: Database cursor object.
    conn: Database connection object.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error in create_tables: {e}")
            conn.rollback()

def main():
    """
    - Establishes a connection to the database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed.  
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        cur.close()
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
