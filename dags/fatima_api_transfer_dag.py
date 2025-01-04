
from __future__ import annotations

import pendulum
import requests
import csv
import json
import io
import re
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 1
}

with DAG(
    dag_id="ftransfer_dag_api_to_bigquery",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    tags=["api", "gcs", "csv", "upload"],
) as ftransfer_dag_api_to_bigquery:
    
    API_URL = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

def fetch_api_to_gcs(api_url, bucket_name, object_name):
        if not (api_url and bucket_name and object_name and re.match(r"^https?://", api_url)):
            return "error"
        try:
            r = requests.get(api_url, stream=True, timeout=10)
            r.raise_for_status()
            if not r.text:
                GCSHook().upload(bucket_name, object_name, "")
                return "warning"
            if r.headers.get('Content-Type') == 'application/json':
                data = r.json()
                if isinstance(data, list) and data and all(isinstance(i, dict) for i in data):
                    buf = io.StringIO()
                    w = csv.DictWriter(buf, fieldnames=data[0].keys(), quoting=csv.QUOTE_NONNUMERIC)
                    w.writeheader()
                    w.writerows(data)
                    GCSHook().upload(bucket_name, object_name, buf.getvalue(), mime_type='text/csv')
                elif isinstance(data, dict):
                    buf = io.StringIO()
                    w = csv.DictWriter(buf, fieldnames=data.keys(), quoting=csv.QUOTE_NONNUMERIC)
                    w.writeheader()
                    w.writerow(data)
                    GCSHook().upload(bucket_name, object_name, buf.getvalue(), mime_type='text/csv')
                elif not data:
                    GCSHook().upload(bucket_name, object_name, "")
                    return "warning"
                else:
                    return "error" #Simplified this line
            else:
                GCSHook().upload(bucket_name, object_name, r.content, mime_type='text/csv')
            return "success"
        except Exception:
            return "error"

fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_api_to_gcs,
        op_kwargs={
            "api_url": "API_URL",
            "bucket_name": "GCS_BUCKET",
            "object_name": "GCS_FILE_PATH",
        },
    )

api_load_to_bigquery = GCSToBigQueryOperator(
        task_id="api_load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    #     schema= [
    #     SchemaField("order_id", "STRING", mode="REQUIRED"),
    #     SchemaField("payment_sequential", "INTEGER"),
    #     SchemaField("payment_type", "STRING"),
    #     SchemaField("payment_installments", "INTEGER"),
    #     SchemaField("payment_value", "FLOAT")
    # ]
 
    )

fetch_and_upload >> api_load_to_bigquery