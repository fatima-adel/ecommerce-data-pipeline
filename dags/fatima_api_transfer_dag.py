from __future__ import annotations
import requests
import json
import pendulum
import requests
import csv
import json
import logging
import io
import validators
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 1
}

with DAG(
    dag_id="ftransfer_dag_api_to_bigquery",
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    schedule_interval=None
) as ftransfer_dag_api_to_bigquery:
    
    API_URL = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

    def fetch_api_to_gcs(api_url, bucket_name, object_name):
        """Fetches API data and writes it directly to GCS."""
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

        if not api_url:
            logging.error("API URL is empty.")
            return "error"
        if not bucket_name:
            logging.error("GCS Bucket name is empty.")
            return "error"
        if not object_name:
            logging.error("Output object name (filename) is empty.")
            return "error"
        if not validators.url(api_url):
            logging.error("Invalid API URL format.")
            return "error"

        try:
            logging.info(f"Fetching data from {api_url}")
            response = requests.get(api_url, stream=True, timeout=10)
            response.raise_for_status()

            if not response.text:
                logging.warning("Empty response received from API. Creating empty GCS object.")
                GCSHook().upload(bucket_name=bucket_name, object_name=object_name, data="")
                return "warning"

            if response.headers.get('Content-Type') == 'application/json':
                data = response.json()
                if isinstance(data, list):
                    if data and all(isinstance(item, dict) for item in data):
                        csv_buffer = io.StringIO()
                        writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
                        GCSHook().upload(bucket_name=bucket_name, object_name=object_name, data=csv_buffer.getvalue(), mime_type='text/csv')
                    elif not data:
                        logging.warning("No data returned from API, creating an empty GCS Object")
                        GCSHook().upload(bucket_name=bucket_name, object_name=object_name, data="")
                        return "warning"
                    else:
                        raise ValueError("The JSON response is not a list of dictionaries")
                elif isinstance(data, dict):
                    csv_buffer = io.StringIO()
                    writer = csv.DictWriter(csv_buffer, fieldnames=data.keys())
                    writer.writeheader()
                    writer.writerow(data)
                    GCSHook().upload(bucket_name=bucket_name, object_name=object_name, data=csv_buffer.getvalue(), mime_type='text/csv')
                else:
                    raise ValueError("The JSON response is neither a list nor a dict")
            else:  # Assume CSV or text
                GCSHook().upload(bucket_name=bucket_name, object_name=object_name, data=response.content, mime_type='text/csv')

            logging.info(f"Data saved to GCS: gs://{bucket_name}/{object_name}")
            return "success"

        except (requests.exceptions.RequestException, json.JSONDecodeError, csv.Error, ValueError) as e:
            logging.exception(f"Error: {e}")
            return "error"
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")
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