import os
import logging
import pandas as pd
from datetime import datetime, timedelta

from src.etl_from_news_api import NewsETL
from src.etl_from_yfinance import StockSpecificETL

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
LOCAL_DATA_COLLECTION_FOLDER = f"{PATH_TO_LOCAL_HOME}/data-collection"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'news_data')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

TICKER_LIST = ['FB', 'AMZN', 'AAPL', 'NFLX', 'GOOGL', 'MSFT', 'TSLA']

logging.getLogger().setLevel(logging.INFO)

# for BashOperator
command_for_create_today_folder = """
today_date={{ ti.xcom_pull(task_ids='get_triggering_date') }};
cd /opt/airflow;
mkdir -p data-collection && mkdir -p data-collection/data-collection-${today_date};
echo "Created 'data-collection-$today_date' folder as a data collection directory.";
"""

# for PythonOperator
def _main_extract_and_transform(tickers, ti):
    # get triggering date and time from xcom
    date = ti.xcom_pull(key='return_value', task_ids='get_triggering_date')
    time = ti.xcom_pull(key='return_value', task_ids='get_triggering_time')

    # get news headlines from NewsAPI
    news_from_news_api = NewsETL().run()
    
    # get news headlines from yfinance 
    news_from_yfinance = StockSpecificETL(tickers).run_news()
    
    # gather two news sources
    gathered_news = pd.concat([news_from_news_api, news_from_yfinance])

    # create a csv to store the hourly data
    csv_location = f'data-collection-{date}/news_headlines-{date}-{time}'
    
    gathered_news.to_csv(f'{LOCAL_DATA_COLLECTION_FOLDER}/{csv_location}.csv', index=False)
    logging.info(f'{csv_location}.csv is successfully generated.')

    # push to exact file location to xcom (ensure the file name is correct)
    ti.xcom_push(key='csv_location', value=f'{csv_location}.csv')

    return None

def _main_load(bucket, ti):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    object_name = ti.xcom_pull(key='csv_location', task_ids='extract_and_transform')
    local_file = f"{LOCAL_DATA_COLLECTION_FOLDER}/{object_name}"

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    return None

default_args = {
    "owner": "airflow",
    "retries": 3,
    'retry_delay': timedelta(minutes=5),
    "start_date": datetime(2022,1,1)
}

with DAG(
    dag_id="get_news_headlines",
    default_args=default_args,
    catchup=False,
    schedule_interval="05 04-20 * * 1-5",
) as dag:

    get_triggering_date = BashOperator(
        task_id="get_triggering_date",
        bash_command="echo $(TZ=America/New_York date +'%Y-%m-%d')",
        do_xcom_push=True
    )

    get_triggering_time = BashOperator(
        task_id="get_triggering_time",
        bash_command="echo $(TZ=America/New_York date +'%H%M%S')",
        do_xcom_push=True
    )
    
    # need branch here...if yes: echo " "  >> ${today_date}_is_market_close_day.txt && end of today workflow
    """check_market_close = BranchPythonOperator(
        task_id='check_market_close')"""
    
    create_today_folder = BashOperator(
        task_id='create_today_folder',
        bash_command=command_for_create_today_folder,
        do_xcom_push=False
    )

    extract_and_transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=_main_extract_and_transform,
        op_kwargs={"tickers": TICKER_LIST}
    )
    
    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_main_load,
        op_kwargs={'bucket': BUCKET}
    )

    get_triggering_date >> get_triggering_time >> create_today_folder >> extract_and_transform >> load_to_gcs