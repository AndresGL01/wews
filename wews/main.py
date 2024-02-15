import os
import re
import socket
import uuid
from datetime import datetime

import requests
from dotenv import load_dotenv

from google.oauth2 import service_account
import polars as pl

from wews.loader.base import Drive
from wews.sources.base import Kaggle, UrlHaus

import tldextract

load_dotenv()


def treat(value: str):
    return tldextract.extract(value).domain


def is_ipv4(value: str) -> bool:
    ip_pattern = r'^(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])){3}$'
    return re.match(ip_pattern, value) is not None


def get_ip(domain: str) -> str or None:
    try:
        return socket.gethostbyname(domain)
    except socket.error as e:
        return None


def process(value: str) -> str:
    return treat(value) if not is_ipv4(value) else get_ip(value)


def get_country(ip: str) -> str | None:
    url = f"https://ipinfo.io/{ip}/json"
    response = requests.get(url)

    if response.status_code == 200:
        payload = response.json()
        return payload.get('country', 'No disponible')

    return None


if __name__ == '__main__':
    scopes = ['https://www.googleapis.com/auth/drive']
    service_account_file = '/home/winters/apps/wews/wews/wews-402510-7ad9cc194bdf.json'

    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scopes)

    df = UrlHaus().get()

    print(df)

    # drive = Drive(credentials=credentials)
    #
    # gold = pl.read_parquet('2024_01_18_gold.parquet')
    # domains = gold.select('domain').unique()
    # ips = gold.head(20).with_columns(
    #     (pl.col('url').map_elements(process)).alias('ip')
    # ).select([pl.col('domain'), pl.col('ip')])
    #
    # countries = ips.with_columns(
    #     (pl.col('ip').map_elements(get_country)).alias('country')
    # ).select([pl.col('ip'), pl.col('country')])
    #
    # dataframes = {
    #     'general': gold,
    #     'domains': domains,
    #     'countries': countries
    # }
    #
    # print(dataframes)
    #
    # today = datetime.now().strftime("%Y_%m_%d")
    # target_id = drive.create_folder(folder_name=today)
    #
    # target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}'

    # for key in dataframes.keys():
    #     filename = f'{key}_{today}.parquet'
    #     specific_target = f'{target_path}{filename}'
    #
    #     df = dataframes.get(key)
    #     df.write_parquet(specific_target)
    #     drive.push_file(specific_target, target_id, f'{key}_{filename}')
    # static = drive.retrieve_static_data()
    #
    # kaggle_spec = [file for file in static if file.get('id') == '1B4_yhwswIM22R2haNQKMV-_lUZg4u51h'][0]
    # drive.get_resource(resource_id=kaggle_spec.get('id'), resource_name=kaggle_spec.get('name'))
    #
    # dataframes = [
    #     UrlHaus().get(),
    #     Kaggle().get(
    #         file_spec=kaggle_spec,
    #     )
    # ]
    # mix = pl.concat(dataframes, how="diagonal")
    #
    # filename = f'{datetime.now().strftime("%Y_%m_%d")}_bronze.parquet'
    # target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{filename}'
    # mix.write_parquet(target_path)
    #
    # drive.push_file(target_path, os.getenv('WAREHOUSE_BRONZE_FOLDER_ID'), filename)
    #
    # predicate = pl.when(pl.col('threat').is_not_null()).then(pl.col('threat')).otherwise(pl.col('type'))
    #
    # silver_dataframe = mix.drop(
    #     ['last_online', 'tags', 'id', 'urlhaus_link', 'url_status', 'dateadded', 'reporter']).with_columns(
    #     predicate.alias('threat_type')
    # ).drop(['type', 'threat'])
    #
    # silver_dataframe = silver_dataframe.with_columns(
    #     pl.col('url').map_elements(treat).alias('domain')
    # )
    #
    # print(silver_dataframe)

    # dataframes = []
    # for file in static_data:
    #     drive.get_resource(file.get('id'), file.get('name'))
    #     df = source_to_dataframe(f"{os.getenv('TEMPORAL_STATIC_DATA_PATH')}{file.get('name')}")
    #     dataframes.append(df)
    # mix = pl.concat(dataframes, how="diagonal")
    # filename = f'{datetime.now().strftime("%Y_%m_%d")}.parquet'
    # target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{filename}'
    # mix.write_parquet(target_path)
    #
    # drive.push_bronze_data(target_path, os.getenv('WAREHOUSE_BRONZE_FOLDER_ID'), filename)
    #
    #
    # predicate = pl.when(pl.col('threat').is_not_null()).then(pl.col('threat')).otherwise(pl.col('type'))
    #
    # mix = mix.drop(['last_online', 'tags', 'id', 'urlhaus_link', 'url_status', 'dateadded', 'reporter']).with_columns(
    #     predicate.alias('threat_type')
    # ).drop(['type', 'threat'])
    #
    # print(mix['threat_type'].unique())
