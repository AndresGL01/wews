import pathlib

import polars as pl


def source_to_dataframe(filepath: str) -> pl.DataFrame:
    suffix = pathlib.Path(filepath).suffix
    if suffix == '.csv':
        return pl.read_csv(filepath)
    if suffix == '.json':
        return pl.read_json(filepath)
