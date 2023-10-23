import os

from dotenv import load_dotenv

from wews.extractor.base import source_to_dataframe
from wews.loader.base import Drive
import polars as pl

load_dotenv()

if __name__ == '__main__':
    drive = Drive()
    static_data = drive.retrieve_static_data()
    dataframes = []
    for file in static_data:
        drive.get_resource(file.get('id'), file.get('name'))
        df = source_to_dataframe(f"{os.getenv('TEMPORAL_STATIC_DATA_PATH')}{file.get('name')}")
        dataframes.append(df)



