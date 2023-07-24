from pathlib import Path
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os 

@task(log_prints=True, retries=3)
def extract_from_gcs(colour: str, year: int, month:int) -> Path:
    '''Download trip data from GCS'''
    gcs_filename = f'{colour}_tripdata_{year}-{month:02}.parquet'
    gcs_path = f'data/{colour}/{colour}_tripdata_{year}-{month:02}.parquet' 
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(
        from_path=gcs_path
        ,local_path=f'/home/arith/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/green/flows/'
    )
    return Path(f'/home/arith/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/green/flows/{gcs_path}')


@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(path)
    print(f'pre: missing passenger count: {df["passenger_count"].isna().sum()}')
    df['passenger_count'].fillna(0, inplace=True)
    print(f'post: missing passenger count: {df["passenger_count"].isna().sum()}') 
    return df 

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    '''Write DataFrame to BigQuery'''

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table='trips_data_all.rides'
        ,project_id='dtc-de-akf'
        ,credentials=gcp_credentials_block.get_credentials_from_service_account() 
        ,chunksize=10_000
        ,if_exists='append'
    )

@flow()
def etl_gcs_to_bq():
    '''Main ETL flow to load data into Big Query'''
    colour = 'green'
    year = 2021
    month =1

    path = extract_from_gcs(colour, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()