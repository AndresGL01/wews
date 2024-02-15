import abc
import os
import requests

import polars as pl


class Source(abc.ABC):
    @abc.abstractmethod
    def get(self, **kwargs) -> pl.DataFrame:
        """
        Applies the logic to retrieve the data and return a polars dataframe
        """


class UrlHaus(Source):
    url = 'https://urlhaus.abuse.ch/api/downloads/json_recent'

    def get(self, **kwargs) -> pl.DataFrame:
        payload = requests.get(
            url=self.url
        )

        return pl.from_dicts(map(lambda a: dict(id=a[0], *a[1]), payload.json().items()))


class Kaggle(Source):
    def get(self, **kwargs) -> pl.DataFrame:
        file_spec: dict = kwargs.get('file_spec')

        return pl.read_csv(f"{os.getenv('TEMPORAL_STATIC_DATA_PATH')}{file_spec.get('name')}")
