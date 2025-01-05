from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import requests
import csv
import json
import logging
import os, io
import requests
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from csv import DictWriter, writer

default_args = {
    "retries": 1
}

with DAG(
    dag_id="ftransfer_dag_api_to_bigquery",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 4, 20),
    tags=["fatima", "api", "gcs", "upload"],
    ) as ftransfer_dag_api_to_bigquery:
    
    API_URL = "/order_payments_table" #https://us-central1-ready-de-25.cloudfunctions.net
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

def upload_to_gcs(api_url, output_filename=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info(f"Fetching data from {api_url}")
    try:
        response = requests.get(api_url, stream=True, timeout=10)
        response.raise_for_status()

        # Create directory if needed
        if output_filename:
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        # Process the data
        if response.headers.get('Content-Type') == 'application/json':
            data = response.json()
            if isinstance(data, (list, dict)):
                output = io.StringIO()
                writer = DictWriter(output, fieldnames=data[0].keys() if isinstance(data, list) else data.keys())
                writer.writeheader()
                for item in data:
                    writer.writerow(item)
                csv_data = output.getvalue()
            else:
                logging.warning("Unexpected JSON format, skipping CSV conversion")

        else:
            # Handle CSV content directly from response (if applicable)
            output = io.StringIO()
            writer = writer(output)
            for line in response.iter_lines(decode_unicode=True):
                writer.writerow(line.split(','))
            csv_data = output.getvalue()

        # Upload data to GCS
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # Specify a different connection ID if needed
        gcs_hook.upload(
            bucket_name='GCS_BUCKET',  # Replace with your bucket name
            object_name='GCS_FILE_PATH',  # Replace with the desired file path
            data=csv_data

        logging.info(f"Data uploaded to GCS: {gcs_hook.upload}")

    except Exception as e:
        logging.error(f"Error during data processing or upload: {e}")
        )

        # Task to fetch API data
    fetch_api_data = SimpleHttpOperator(
        task_id='fetch_api_data',
        http_conn_id='http_default',  # Connection ID for your HTTP API
        endpoint=API_URL,
        method='GET',
        dag=ftransfer_dag_api_to_bigquery  # Ensure DAG is passed explicitly
    )

    # Task to upload data to GCS
    upload_csv_to_gcs = PythonOperator(
        task_id='upload_csv_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
        dag=ftransfer_dag_api_to_bigquery  # Ensure DAG is passed explicitly
    )

    api_load_to_bigquery = GCSToBigQueryOperator(
        task_id="api_load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        dag=ftransfer_dag_api_to_bigquery  # Ensure DAG is passed explicitly
    )

    # Define task dependencies
    fetch_api_data >> upload_csv_to_gcs >> api_load_to_bigquery