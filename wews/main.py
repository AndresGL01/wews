import os

from dotenv import load_dotenv

from google.oauth2 import service_account



from wews.extractor.base import source_to_dataframe
from wews.loader.base import Drive
import polars as pl

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
