import os
import re
import shutil
import socket

import pendulum
import polars
import polars as pl
import requests

from airflow.decorators import dag, task
import tldextract

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
        """
        Authenticate to the corresponding services
        """
        scopes = ['https://www.googleapis.com/auth/drive']
        service_account_file = os.getenv('GOOGLE_CREDS_PATH')

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
        """
        Extract the dynamic data, mix the dataframes and push it to the data lake
        """
        kaggle_spec = [file for file in static_data if file.get('id') == os.getenv('KAGGLE_ID')][0]
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
    def transform(bronze_dataframe: polars.DataFrame) -> dict:
        """
        Clean columns and create new ones
        """

        def treat(value: str) -> str:
            """
            Return the url domain
            """
            return tldextract.extract(value).domain

        def get_ip(domain: str) -> str or None:
            try:
                return socket.gethostbyname(domain)
            except socket.error as e:
                return None

        def process(value: str) -> str:
            return get_ip(treat(value))

        def get_country(ip: str) -> str | None:
            url = f"https://ipinfo.io/{ip}/json"
            response = requests.get(url)

            if response.status_code == 200:
                payload = response.json()
                return payload.get('country', 'No disponible')

            return None

        predicate = pl.when(pl.col('threat').is_not_null()).then(pl.col('threat')).otherwise(pl.col('type'))

        df = bronze_dataframe.drop(
            ['last_online', 'tags', 'id', 'urlhaus_link', 'url_status', 'dateadded', 'reporter']).with_columns(
            predicate.alias('threat_type')
        ).drop(['type', 'threat'])

        df = df.with_columns(
            pl.col('url').map_elements(treat).alias('domain')
        )

        domains = df.select('domain').unique()
        ips = df.sample(n=1000).with_columns(
            (pl.col('url').map_elements(process)).alias('ip')
        )

        countries = ips.with_columns(
            (pl.col('ip').map_elements(get_country)).alias('country')
        )

        return {
            'general': df,
            'domains': domains,
            'ips': ips,
            'countries': countries
        }

    @task()
    def load(dataframes: dict, drive_client: Client) -> None:
        """
        Takes the silver dataframe and push it to the data lake
        """
        today = datetime.now().strftime("%Y_%m_%d")
        target_id = drive_client.create_folder(folder_name=today)

        target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}'

        for key in dataframes.keys():
            filename = f'{key}_{today}.parquet'
            specific_target = f'{target_path}{filename}'

            df = dataframes.get(key)
            df.write_parquet(specific_target)
            drive_client.push_file(specific_target, target_id, filename)

        cleanup()

    def cleanup() -> None:
        """
        Remove all static data/auxiliar files in tmp folder
        """
        shutil.rmtree(os.getenv("TEMPORAL_STATIC_DATA_PATH"))

    drive = authenticate()
    static_data = set_up(drive)
    bronze = extract(static_data, drive)
    silver = transform(bronze)
    load(silver, drive)


etl()
