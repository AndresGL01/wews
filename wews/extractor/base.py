import json
import pathlib

import polars as pl


def source_to_dataframe(filepath: str) -> pl.DataFrame:
    suffix = pathlib.Path(filepath).suffix
    if suffix == '.csv':
        return pl.read_csv(filepath)
    if suffix == '.json':
        data = json.loads(pathlib.Path(filepath).read_text())
        df = pl.from_dicts(map(lambda a: dict(id=a[0], *a[1]), data.items()))
        return df
