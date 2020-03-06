import click
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Used to call drop_table_queries which, as the name suggests,
    specify how and which tables are going to be dropped.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Used to call create_table_queries which, as the name suggests,
    specify how and which tables are going to be created.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST                   = config.get('CLUSTER', 'HOST')
    DB_NAME                = config.get('CLUSTER', 'DB_NAME')
    DB_USER                = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD            = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT                = config.get('CLUSTER', 'DB_PORT')
        
    conn = psycopg2.connect(
        f'host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}'
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()