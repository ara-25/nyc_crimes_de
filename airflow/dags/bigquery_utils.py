import os

from airflow.macros import ds_format
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

schema_dict = {
    "cmplnt_to_dt": "DATETIME",
    "rpt_dt": "DATETIME",
    "ofns_desc": "STRING",
    "prem_typ_desc": "STRING",
    "y_coord_cd": "INTEGER",
    "x_coord_cd": "INTEGER",
    "cmplnt_fr_tm": "TIME",
    "latitude": "FLOAT",
    "addr_pct_cd": "STRING",
    "cmplnt_to_tm": "TIME",
    "crm_atpt_cptd_cd": "STRING",
    "cmplnt_num": "STRING",
    "longitude": "FLOAT",
    "law_cat_cd": "STRING",
    "pd_desc": "STRING",
    "pd_cd": "STRING",
    "ky_cd": "STRING",
    "boro_nm": "STRING",
    "cmplnt_fr_dt": "DATETIME",
    "juris_desc": "STRING",
    "loc_of_occur_desc": "STRING",
}
schema = [bigquery.SchemaField(field, dtype) for field, dtype in schema_dict.items()]


def load_csv_bq(bq_table, ds, **kwargs):
    year = ds_format(ds, "%Y-%m-%d", "%Y")
    month = ds_format(ds, "%Y-%m-%d", "%m")
    year = int(year)
    quarter = (int(month) - 1) // 3
    file_name = f"{year}|{quarter}.csv"
    file_uri = f"gs://{BUCKET}/crime_data/{file_name}"
    table_id = f"{PROJECT_ID}.crime_data.{bq_table}"

    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
    )    

    load_job = client.load_table_from_uri(
        file_uri, table_id, job_config=job_config
    )
    load_job.result()
