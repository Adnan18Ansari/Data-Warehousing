import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_staging_order
                        count_staging_queries, insert_table_order, count_fact_dim_queries


def load_staging_tables(cur, conn):
    """
    Description: Copies data in json format in S3 to staging tables in redshift.
    """
    for idx, query in enumerate(copy_table_queries):
        cur.execute(query)
        conn.commit()
        row = cur.execute(count_staging_queries[idx])
        print('No. of rows copied into {}: {}'.format(copy_staging_order[idx], row.count))


def insert_tables(cur, conn):
    """
    Description: ETL from staging tables to songplays fact and its dimension 
                 tables in redshift.
    """
    for idx, query in enumerate(insert_table_queries):
        cur.execute(query)
        conn.commit()
        row = cur.execute(count_fact_dim_queries[idx])
        print('No. of rows inserted into {}: {}'.format(insert_table_order[idx], row.count))


def main():
    """
    Description:
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