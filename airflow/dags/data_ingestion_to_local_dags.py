import os
#import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_data import main as ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_2021-01.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output.parquet'
TABLE_NAME = "yellow_taxi"

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

with DAG(
    dag_id="local_ingestion_dags",
    schedule_interval="@daily",
    start_date=datetime(2022,6,4)
) as dag:

    wget_task = BashOperator(
        task_id="curl_data_set",
        bash_command=f"curl -sS {URL_TEMPLATE} > {AIRFLOW_HOME}/output.parquet"
    )

    check_task = BashOperator(
        task_id="check_exist",
        bash_command=f"ls -al /opt/airflow"
    )

    ingest_task = PythonOperator(
        task_id="ingest_data_set",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME,
            csv_file=OUTPUT_FILE_TEMPLATE
        )
    )

    wget_task >> check_task >> ingest_task