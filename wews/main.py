import os

from dotenv import load_dotenv

from google.oauth2 import service_account
import polars as pl

from wews.extractor.base import source_to_dataframe
from wews.loader.base import Drive

from datetime import datetime

load_dotenv()

if __name__ == '__main__':

    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = '../wews-402510-7ad9cc194bdf.json'

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    drive = Drive(credentials=credentials)
    static_data = drive.retrieve_static_data()
    dataframes = []
    for file in static_data:
        drive.get_resource(file.get('id'), file.get('name'))
        df = source_to_dataframe(f"{os.getenv('TEMPORAL_STATIC_DATA_PATH')}{file.get('name')}")
        dataframes.append(df)
    mix = pl.concat(dataframes, how="diagonal")
    filename = f'{datetime.now().strftime("%Y_%m_%d")}.parquet'
    target_path = f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{filename}'
    mix.write_parquet(target_path)

    drive.push_bronze_data(target_path, os.getenv('WAREHOUSE_BRONZE_FOLDER_ID'), filename)
