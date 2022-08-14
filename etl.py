import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import datetime

def load_staging_tables(cur, conn):
    startTime = datetime.datetime.now()
    for query in copy_table_queries:
        print("S3 Transfer to Staging Query Started")
        cur.execute(query)
        conn.commit()
        print("S3 Staging Transfer Query Completed in {} seconds.".\
              format(datetime.datetime.now() - startTime))
        startTime = datetime.datetime.now()
    print("Data Successfully copied from S3 to all staging tables.\n\n\n")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        startTime = datetime.datetime.now()
        print("Transfer Query Started from staging table")
        cur.execute(query)
        conn.commit()
        print("Transfer from Staging table Completed in {} seconds.\n".\
              format(datetime.datetime.now() - startTime))
        startTime = datetime.datetime.now()
    print("Data Sucessfully inserted into All warehouse tables\
          from staging tables\n\n")


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