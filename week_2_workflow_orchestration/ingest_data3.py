#!/usr/bin/env python
# coding: utf-8
import os
from sqlalchemy import create_engine
import pandas as pd
from time import time
import argparse
import pyarrow



# user, password, host, port, database name, table name, url of the csv 

def main(user, password, host, port, db, table_name, url):
    csv_name = 'output.csv'
    parquet_name = 'input.parquet'
    # download the csv 

    os.system(f"wget {url} -O {parquet_name}")
    # Read the Parquet file into a DataFrame
    # Read the Parquet file into a DataFrame
    parquet_file = rf'{parquet_name}'
    df_parq = pd.read_parquet(parquet_file, engine= 'pyarrow')

    # Convert the DataFrame to CSV
    df_parq.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try:
            t_start = time()

            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        
        except StopIteration:
            print('Finished ingesting data into postgres database')
            break


if __name__ == '__main__':
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = 5432
    db = 'ny_taxi'
    table_name = 'green_taxi_trips'
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"

    main(user, password, host, port, db, table_name, url)
