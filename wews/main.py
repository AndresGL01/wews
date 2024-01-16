import os
import shutil

from dotenv import load_dotenv

from google.oauth2 import service_account
import polars as pl

from wews.loader.base import Drive


load_dotenv()

if __name__ == '__main__':
    shutil.rmtree(os.getenv("TEMPORAL_STATIC_DATA_PATH"))
    # SCOPES = ['https://www.googleapis.com/auth/drive']
    # SERVICE_ACCOUNT_FILE = '/home/winters/apps/wews/wews/wews-402510-7ad9cc194bdf.json'
    #
    # credentials = service_account.Credentials.from_service_account_file(
    #     SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    #
    # drive = Drive(credentials=credentials)
    # static_data = drive.retrieve_static_data()

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
