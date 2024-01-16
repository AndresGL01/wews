import os
import shutil
from datetime import datetime

from dotenv import load_dotenv

from google.oauth2 import service_account
import polars as pl

from wews.loader.base import Drive
from wews.sources.base import Kaggle, UrlHaus

import tldextract

load_dotenv()


def treat(value: str):
    return tldextract.extract(value).domain


if __name__ == '__main__':
    scopes = ['https://www.googleapis.com/auth/drive']
    service_account_file = '/home/winters/apps/wews/wews/wews-402510-7ad9cc194bdf.json'

    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scopes)

    drive = Drive(credentials=credentials)
    static = drive.retrieve_static_data()

    kaggle_spec = [file for file in static if file.get('id') == '1B4_yhwswIM22R2haNQKMV-_lUZg4u51h'][0]
    drive.get_resource(resource_id=kaggle_spec.get('id'), resource_name=kaggle_spec.get('name'))

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

    drive.push_file(target_path, os.getenv('WAREHOUSE_BRONZE_FOLDER_ID'), filename)

    predicate = pl.when(pl.col('threat').is_not_null()).then(pl.col('threat')).otherwise(pl.col('type'))

    silver_dataframe = mix.drop(
        ['last_online', 'tags', 'id', 'urlhaus_link', 'url_status', 'dateadded', 'reporter']).with_columns(
        predicate.alias('threat_type')
    ).drop(['type', 'threat'])

    silver_dataframe = silver_dataframe.with_columns(
        pl.col('url').map_elements(treat).alias('domain')
    )

    print(silver_dataframe)

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
