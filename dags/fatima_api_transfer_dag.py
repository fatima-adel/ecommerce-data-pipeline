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
    schedule=None,
    start_date=datetime(2024, 4, 20),
    tags=["api","fatima", "gcs", "transfer"],
) as ftransfer_dag_api_to_bigquery:

    API_BASE_URL = "https://us-central1-ready-de-25.cloudfunctions.net" # Define the base URL
    API_ENDPOINT = "/order_payments_table"  # Define the endpoint
    API_URL = API_BASE_URL + API_ENDPOINT # Combine base and endpoint for full URL
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

    def upload_to_gcs(api_url, bucket, obj):
        if not (api_url and bucket and obj and re.match(r"^https?://", api_url)):
            print("Invalid input parameters.")  # Use print for basic debugging in Airflow
            return "error"
        try:
            print(f"Fetching from: {api_url}") # Print the URL being fetched
            r = requests.get(api_url, stream=True, timeout=10)
            r.raise_for_status()
            print(f"Response status code: {r.status_code}")
            
            if not r.text:
                print("Empty response from API.")
                GCSHook().upload(bucket, obj, "")
                return "warning"

            content_type = r.headers.get('Content-Type', '')
            print(f"Content-Type: {content_type}")

            if "application/json" in content_type:
                data = r.json()
                if isinstance(data, (list, dict)) and data:
                    f = data[0].keys() if isinstance(data, list) else data.keys()
                    if f:
                        buf = io.StringIO()
                        w = csv.DictWriter(buf, fieldnames=f, quoting=csv.QUOTE_NONNUMERIC)
                        w.writeheader()
                        w.writerows(data) if isinstance(data, list) else w.writerow(data)
                        GCSHook().upload(bucket=GCS_BUCKET, obj=GCS_FILE_PATH, data=buf.getvalue(), mime_type='text/csv')
                        print(f"Uploaded to gs://{bucket}/{obj}")
                    else:
                        print("No fields found in JSON data, creating empty file.")
                        GCSHook().upload(bucket,obj,"")
                        return "warning"
                elif not data:
                    print("JSON data is not a list or dict, creating empty file.")
                    GCSHook().upload(bucket, obj, "")
                    return "warning"
                else:
                    return "error"
            else:
                GCSHook().upload(bucket, obj, r.content.decode('utf-8',errors='replace'), mime_type='text/csv')
                print(f"Uploaded to gs://{bucket}/{obj}")
            return "success"
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return "error"
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}. Response text: {r.text if 'r' in locals() else 'No response text'}")
            return "error"
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return "error"

    fetch_api_data = SimpleHttpOperator(
        task_id='fetch_api_data',
        http_conn_id='http_default',
        endpoint=API_ENDPOINT, # Use the endpoint here
        method='GET',
        dag=ftransfer_dag_api_to_bigquery
    )

    upload_csv_to_gcs = PythonOperator(
        task_id='upload_csv_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            "api_url": API_URL, # Pass the full API URL here
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