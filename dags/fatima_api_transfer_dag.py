from __future__ import annotations

import datetime
import requests
import csv
import json
import io
import re
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

with DAG(
    dag_id="ftransfer_dag_api_to_bigquery",
    schedule=None,
    start_date=datetime(2023, 10, 26),  # Use a past start date for testing
    catchup=False,
    tags=["api", "fatima", "gcs",  "transfer"],
) as ftransfer_dag_api_to_bigquery:

    api_url = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

    def upload_to_gcs(api_url, bucket, obj):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        if not (api_url and bucket and obj and re.match(r"^https?://", api_url)):
            logging.error("Invalid input parameters.")
            return "error"
        try:
            logging.info(f"Fetching from: {api_url}")
            r = requests.get(api_url, stream=True, timeout=10)
            r.raise_for_status()
            logging.info(f"Response status code: {r.status_code}")

            if not r.text:
                logging.warning("Empty response from API.")
                GCSHook().upload(bucket, obj, "")
                return "warning"

            content_type = r.headers.get('Content-Type', '')
            logging.info(f"Content-Type: {content_type}")

            if "application/json" in content_type:
                data = r.json()
                if isinstance(data, (list, dict)) and data:
                    f = data[0].keys() if isinstance(data, list) else data.keys()
                    if f:
                        buf = io.StringIO()
                        w = csv.DictWriter(buf, fieldnames=f, quoting=csv.QUOTE_NONNUMERIC)
                        w.writeheader()
                        if isinstance(data, list):
                            w.writerows(data)
                        else:
                            w.writerow(data)
                        csv_data = buf.getvalue()
                    else:
                        logging.warning("No fields found in JSON data, creating empty file.")
                        csv_data = ""
                        GCSHook().upload(bucket,obj,"")
                        return "warning"
                else:
                    logging.warning("JSON data is not a list or dict, creating empty file.")
                    csv_data = ""
                    GCSHook().upload(bucket, obj, "")
                    return "warning"

            else:
                csv_data = r.content.decode('utf-8', errors='replace') # Decode with error handling
                logging.info("Treating response as non-JSON (likely CSV).")


            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            gcs_hook.upload(bucket, obj, csv_data, mime_type='text/csv')
            logging.info(f"Uploaded to gs://{bucket}/{obj}")
            return "success"
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            return "error"
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}. Response text: {r.text if 'r' in locals() else 'No response text'}")
            return "error"
        except Exception as e:
            logging.exception(f"An unexpected error occurred: {e}")
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