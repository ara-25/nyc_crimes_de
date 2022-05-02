import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator, DataprocSubmitJobOperator)
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BQ_TABLE_NYC_CRIMES = os.environ.get("BQ_TABLE_NYC_CRIMES")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
REGION = "us-west1"
ZONE = "us-west1-a"
CLUSTER_NAME = "dezc-dproc-cluster"

PYSPARK_MAIN_FILE = "test.py"
PYSPARK_MAIN_FILE_PATH = os.path.join(AIRFLOW_HOME, "dags", PYSPARK_MAIN_FILE)
SPARK_BQ_JAR = "spark-bigquery-latest_2.12.jar"
SPARK_BQ_JAR_PATH = os.path.join(AIRFLOW_HOME, "dags", SPARK_BQ_JAR)


CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
            project_id=PROJECT_ID,
            zone=ZONE,
            master_machine_type="n1-standard-4",
            idle_delete_ttl=900,                    
            master_disk_size=500,
            num_masters=1, # single node cluster
            num_workers=0,                     
        ).make()


pyspark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/{PYSPARK_MAIN_FILE}",
        "jar_file_uris": [f"gs://{BUCKET}/{SPARK_BQ_JAR}"],
        "args": [
            f"--project_id={PROJECT_ID}",
            f"--bq_table_input={BQ_TABLE_NYC_CRIMES}",
            f"--bucket={BUCKET}"
        ],
    },
}

def upload_to_gcs(local_file, bucket):
    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = os.path.basename(local_file)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_crime_process_dag",
    schedule_interval="@once",
    default_args=default_args,
    tags=['dtc-de-project'],
) as dag:
    
    upload_pyspark_script = PythonOperator(
        task_id="upload_pyspark_script",
        python_callable=upload_to_gcs,
        op_kwargs={
            "local_file": PYSPARK_MAIN_FILE_PATH,
            "bucket": BUCKET,
        },
    )

    upload_jar = PythonOperator(
        task_id="upload_jar",
        python_callable=upload_to_gcs,
        op_kwargs={
            "local_file": SPARK_BQ_JAR_PATH,
            "bucket": BUCKET,
        },
    )

    create_cluster_operator_task = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG
    )
    
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=pyspark_job, region=REGION, project_id=PROJECT_ID
    )
    

    upload_pyspark_script >> upload_jar >> create_cluster_operator_task >> pyspark_task
