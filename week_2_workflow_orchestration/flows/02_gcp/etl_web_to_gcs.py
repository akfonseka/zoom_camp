from pathlib import Path
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
import os 
import pyarrow

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """"Read taxi data from web into pandas Dataframe"""
    df = pd.read_csv(dataset_url)
    return df 

@task(log_prints=True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(f'columns: {df.columns}')
    print(f'dtypes: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df 


@task(retries=3, log_prints=True)
def write_local(df: pd.DataFrame,colour:str, dataset_file: str) -> Path:
    '''Write DataFrame out locally as a parquet file'''
    path = Path(f'~/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/flows/{colour}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    '''Upload local parquet to GCS'''
    gcs_block = GcsBucket.load("zoom-gcs")
    expanded_path = os.path.expanduser(path)
    gcs_block.upload_from_path(
        from_path=f'{expanded_path}'
        ,to_path=f'data/green/green_tripdata_2021-01.parquet'
    )
    return

#Flow is a decorator that wraps around a function
@flow()
def etl_web_to_gcs() -> None:
    """ The main ETL Function"""
    colour = 'green'
    year = 2021
    month = 1
    dataset_file = f'{colour}_tripdata_{year}-{month:02}'    
    # dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
    # df = fetch(dataset_url)
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet'
    csv_name = 'output.csv'
    parquet_name = 'input.parquet'

    os.system(f"wget {url} -O {parquet_name}")
    
    # Read the Parquet file into a DataFrame
    parquet_file = rf'{parquet_name}'
    df_parq = pd.read_parquet(parquet_file, engine= 'pyarrow')

    # Convert the DataFrame to CSV
    df_parq.to_csv(csv_name, index=False)

    df = fetch(csv_name)
    df_clean = clean(df)
    path = write_local(df_clean, colour, dataset_file)
    write_gcs(path)

    
if __name__ == '__main__':
    etl_web_to_gcs()

