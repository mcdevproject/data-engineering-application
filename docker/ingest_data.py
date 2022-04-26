# Script Design: create connect engin -> make df_chunk (chunksize) 
# -> modify data type (to timestamp) -> to database

import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine, table

def main(params):

    # user, password, host, port, database name, table name, .csv download url
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # download the .csv first
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_chunk = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_chunk)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # make table header by using df.head(0)
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    print(f"Start appending {csv_name} to the database...")

    while True:
        try:
            t_start = time()

            df = next(df_chunk)
            
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

            # table name is 'yellow_taxi_trips'
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print(f"Time taken for this chunk: {t_end - t_start}s")

        except StopIteration:
            raise ("Iteration is done.")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest .csv data to Postgres.')

    # user, password, host, port, database name, table name, .csv download url
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
