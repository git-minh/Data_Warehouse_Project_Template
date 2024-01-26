import configparser
import psycopg2
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            start_time = time.time()
            cur.execute(query)
            conn.commit()
            logging.info(f"Table loaded successfully: Query - {query}. Time taken: {time.time() - start_time} seconds")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error loading staging table. Query - {query}. Error: {e}")

def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            start_time = time.time()
            cur.execute(query)
            conn.commit()
            logging.info(f"Data inserted successfully: Query - {query}. Time taken: {time.time() - start_time} seconds")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting into table. Query - {query}. Error: {e}")

def main():
    config = configparser.ConfigParser()

    # Reading configuration file
    try:
        config.read('dwh.cfg')
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        return

    # Establishing database connection
    try:
        with psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) as conn:
            with conn.cursor() as cur:
                load_staging_tables(cur, conn)
                insert_tables(cur, conn)
    except Exception as e:
        logging.error(f"Error establishing database connection: {e}")

if __name__ == "__main__":
    main()
