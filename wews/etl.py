import os
import shutil

import pendulum
import polars
import polars as pl

from airflow.decorators import dag, task

from sources.base import UrlHaus, Kaggle
from loader.base import Drive, Client
from datetime import datetime

from google.oauth2 import service_account


@dag(
    schedule="0 0 */3 * *",
    start_date=pendulum.datetime(2023, 10, 22, 0),
    dag_id="wews",
    tags=["tfg", "security"],
)
def etl():
    @task()
    def authenticate() -> Client:
        scopes = ['https://www.googleapis.com/auth/drive']
        service_account_file = '/home/winters/apps/wews/wews/wews-402510-7ad9cc194bdf.json'

        credentials = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=scopes)

        return Drive(credentials=credentials)

    @task()
    def set_up(client: Client) -> list:
        """
        Download all the static data into tmp folder defined in environment
        """
        return client.retrieve_static_data()

    @task()
    def extract(static_data: list, client: Client) -> polars.DataFrame:
        kaggle_spec = [file for file in static_data if file.get('id') == '1B4_yhwswIM22R2haNQKMV-_lUZg4u51h'][0]
        client.get_resource(resource_id=kaggle_spec.get('id'), resource_name=kaggle_spec.get('name'))

        dataframes = [
            UrlHaus().get(),
            Kaggle().get(
                file_spec=kaggle_spec,
            )
        ]
        mix = pl.concat(dataframes, how="diagonal")

        filename = f'{datetime.now().strftime("%Y_%m_%d")}_bronze.parquet'
        target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{filename}'
        mix.write_parquet(target_path)

        client.push_file(target_path, os.getenv('WAREHOUSE_BRONZE_FOLDER_ID'), filename)

        return mix

    @task()
    def transform(bronze_dataframe: polars.DataFrame) -> polars.DataFrame:
        predicate = pl.when(pl.col('threat').is_not_null()).then(pl.col('threat')).otherwise(pl.col('type'))

        silver_dataframe = bronze_dataframe.drop(
            ['last_online', 'tags', 'id', 'urlhaus_link', 'url_status', 'dateadded', 'reporter']).with_columns(
            predicate.alias('threat_type')
        ).drop(['type', 'threat'])

        return silver_dataframe

    @task()
    def load(silver_dataframe: polars.DataFrame, drive_client: Client) -> None:
        filename = f'{datetime.now().strftime("%Y_%m_%d")}_gold.parquet'
        target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{filename}'
        silver_dataframe.write_parquet(target_path)

        drive_client.push_file(target_path, os.getenv('WAREHOUSE_GOLD_FOLDER_ID'), filename)

        cleanup()

    def cleanup() -> None:
        shutil.rmtree(os.getenv("TEMPORAL_STATIC_DATA_PATH"))

    drive = authenticate()
    static_data = set_up(drive)
    bronze = extract(static_data, drive)
    silver = transform(bronze)
    load(silver, drive)


etl()
