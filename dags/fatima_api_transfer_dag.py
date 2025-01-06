from __future__ import annotations

from datetime import datetime
import requests
import csv
import json
import io
import re

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

with DAG(
    dag_id="ftransfer_dag_api_to_bigquery",
    start_date=datetime(2024, 4, 20),
    schedule=None,
    tags=["api", "fatima", "gcs",  "transfer"],
) as ftransfer_dag_api_to_bigquery:

    api_url = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

    def upload_to_gcs(api_url, bucket, obj):
        if not (api_url and bucket and obj and re.match(r"^https?://", api_url)):
            return "error"
        try:
            r = requests.get(api_url, stream=True, timeout=10)
            r.raise_for_status()
            if not r.text:
                GCSHook().upload(bucket, obj, "")
                return "warning"
            if "application/json" in r.headers.get('Content-Type', ''):
                data = r.json()
                if isinstance(data, (list, dict)) and data:
                    f = data[0].keys() if isinstance(data, list) else data.keys()
                    if f:
                        buf = io.StringIO()
                        w = csv.DictWriter(buf, fieldnames=f, quoting=csv.QUOTE_NONNUMERIC)
                        w.writeheader()
                        w.writerows(data) if isinstance(data, list) else w.writerow(data)
                        GCSHook().upload(bucket, obj, buf.getvalue(), mime_type='text/csv')
                    else:
                        GCSHook().upload(bucket,obj,"")
                        return "warning"
                elif not data:
                    GCSHook().upload(bucket, obj, "")
                    return "warning"
                else:
                    return "error"
            else:
                GCSHook().upload(bucket, obj, r.content.decode('utf-8',errors='replace'), mime_type='text/csv')
            return "success"
        except Exception:
            return "error"

    fetch_api_data = SimpleHttpOperator(
        task_id='fetch_api_data',
        http_conn_id='http_default',
        endpoint="/order_payments_table",
        method='GET',
        dag=ftransfer_dag_api_to_bigquery
    )

    upload_csv_to_gcs = PythonOperator(
        task_id='upload_csv_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            "api_url": api_url,
            "bucket": GCS_BUCKET,
            "obj": GCS_FILE_PATH,
        },
        dag=ftransfer_dag_api_to_bigquery
    )

    api_load_to_bigquery = GCSToBigQueryOperator(
        task_id="api_load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        dag=ftransfer_dag_api_to_bigquery
    )

    fetch_api_data >> upload_csv_to_gcs >> api_load_to_bigquery