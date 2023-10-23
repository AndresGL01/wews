import json
import os
import pathlib

import pendulum
import polars
import polars as pl

from airflow.decorators import dag, task

from wews.extractor.base import get_suffix, source_to_dataframe
from wews.loader.base import Drive


@dag(
    schedule="0 0 */3 * *",
    start_date=pendulum.datetime(2023, 10, 22, 0),
    dag_id="wews",
    tags=["tfg", "security"],
)
def etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    @task()
    def extract() -> polars.DataFrame:
        """

        :return: A combination of n dataframes based on origin data points.
        """
        drive = Drive()
        static_data = drive.retrieve_static_data()
        dataframes = []
        for file in static_data:
            drive.get_resource(file.get('id'), file.get('name'))
            df = source_to_dataframe(f"{os.getenv('TEMPORAL_STATIC_DATA_PATH')}{file.get('name')}")
            dataframes.append(df)

        print(dataframes)


        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


etl()
