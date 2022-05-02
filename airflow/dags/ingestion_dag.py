import datetime
import logging
import os
import os.path as osp

import pandas as pd
from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from sodapy import Socrata

from bigquery_utils import load_csv_bq, schema_dict

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BQ_TABLE_NYC_CRIMES = os.environ.get("BQ_TABLE_NYC_CRIMES")
SOCRATA_TOKEN = os.environ.get("SOCRATA_TOKEN")
LIMIT = 500_000


client = Socrata(
    "data.cityofnewyork.us",
    SOCRATA_TOKEN,
)

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_data_quarter(ds, **kwargs):
    year = ds_format(ds, "%Y-%m-%d", "%Y")
    month = ds_format(ds, "%Y-%m-%d", "%m")
    year = int(year)
    quarter = (int(month) - 1) // 3
    quarter_range = [
        ("01-01", "03-31"),
        ("04-01", "06-30"),
        ("07-01", "09-30"),
        ("10-01", "12-31"),
    ]
    quarter_start, quarter_end = quarter_range[quarter]
    date_start, date_end = f"{year}-{quarter_start}", f"{year}-{quarter_end}"
    df = fetch_results_df(date_start, date_end)
    file_name = f"{year}|{quarter}.csv"
    save_path = osp.join(path_to_local_home, file_name)
    df.to_csv(save_path, index=False)
    
    
def fetch_results_df(date_start, date_end):
    i = 0
    results = []
    while True:
        result = client.get("qgea-i56i", limit=LIMIT, offset=i,
                     where=f"rpt_dt between '{date_start}' and '{date_end}'"
        )
        i += LIMIT
        if len(result) == 0:
            break
        results.append(result)
        
    df = [pd.DataFrame.from_records(r) for r in results]
    df = pd.concat(df, ignore_index=True)

    df = df[list(schema_dict.keys())]
    return df


def upload_to_gcs(bucket, ds, **kwargs):
    year = ds_format(ds, "%Y-%m-%d", "%Y")
    month = ds_format(ds, "%Y-%m-%d", "%m")
    year = int(year)
    quarter = (int(month) - 1) // 3
    file_name = f"{year}|{quarter}.csv"
    local_file = osp.join(path_to_local_home, file_name)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = f"crime_data/{file_name}"
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2015, 1, 1),
    "end_date": datetime.datetime(2020, 2, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_crime_ingestion_dag",
    schedule_interval="5 0 3 1,4,7,10 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-project'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset",
        python_callable=get_data_quarter,
        provide_context=True,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        provide_context=True,
        op_kwargs={
            "bucket": BUCKET,
        },
    )
    
    gcs_to_bq_task = PythonOperator(
        task_id="gcs_to_bq_task",
        python_callable=load_csv_bq,
        provide_context=True,
        op_kwargs={
            "bq_table": BQ_TABLE_NYC_CRIMES
        }
    )

    download_dataset_task >> local_to_gcs_task >> gcs_to_bq_task
