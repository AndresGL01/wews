# How to install

## Requirements

You need at least python 3.10 and poetry, then you can run the following commands:

```sh
poetry install
```
 You'll need Airflow too, install it with:
```sh
pip install "apache-airflow[celery]==2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt"
```
