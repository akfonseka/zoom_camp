#!/usr/bin/env python
# coding: utf-8
import os
from sqlalchemy import create_engine
import pandas as pd
from time import time
import argparse
import pyarrow
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries =3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    csv_name = 'output.csv'
    parquet_name = 'input.parquet'
    # download the csv 

    os.system(f"wget {url} -O {parquet_name}")
    
    # Read the Parquet file into a DataFrame
    parquet_file = rf'{parquet_name}'
    df_parq = pd.read_parquet(parquet_file, engine= 'pyarrow')

    # Convert the DataFrame to CSV
    df_parq.to_csv(csv_name, index=False)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    return df 
# user, password, host, port, database name, table name, url of the csv 

@task(log_prints=True, )
def transform_data(df):
    print(f"pre: missing passenger count {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] !=0]
    print(f"post: missing passenger count {df['passenger_count'].isin([0]).sum()}")
    return df 

@task(log_prints= True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load('postgres-connector')
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="SubFlow", log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging SubFlow for: {table_name}')

@flow(name= "Ingest Flow")
def main_flow(table_name: str):

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)

    ingest_data(table_name, data)

if __name__ == '__main__':
    main_flow("green_taxi_trips")