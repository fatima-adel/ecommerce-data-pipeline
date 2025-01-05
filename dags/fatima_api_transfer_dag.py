from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import csv
import io
import json
from google.cloud import storage
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
    start_date=datetime(2024, 4, 20),
    tags=["fatima", "api", "gcs", "upload"],
    ) as ftransfer_dag_api_to_bigquery:
    
    API_URL = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"
    GCS_BUCKET = "ready-d25-postgres-to-gcs"
    GCS_FILE_PATH = "fatima/order_payments.csv"
    PROJECT_ID = "ready-de-25"
    DATASET_ID = "landing"
    TABLE_ID = "fatima_order_payments"

    def upload_to_gcs(**kwargs):
        # Get the API response from XCom
        api_response = kwargs['ti'].xcom_pull(task_ids='fetch_api_data')

        # Ensure the API response is a list of dictionaries
        if isinstance(api_response, str):
            try:
                # Attempt to parse the string as JSON if it's a string
                api_response = json.loads(api_response)
            except json.JSONDecodeError:
                raise ValueError("API response is a string but could not be parsed as JSON.")
        
        # Check that the response is a list of dictionaries
        if not isinstance(api_response, list) or not all(isinstance(item, dict) for item in api_response):
            raise ValueError("API response is not in the expected format (list of dictionaries).")
        
        # Convert the API response (list of dicts) to CSV
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=api_response[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(api_response)
        output.seek(0)

        # Upload the CSV file to GCS
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # You can specify a different connection ID if needed
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=GCS_FILE_PATH,
            data=output.getvalue()
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
        # schema= [
        #     SchemaField("order_id", "STRING", mode="REQUIRED"),
        #     SchemaField("payment_sequential", "INTEGER"),
        #     SchemaField("payment_type", "STRING"),
        #     SchemaField("payment_installments", "INTEGER"),
        #     SchemaField("payment_value", "FLOAT")
        # ]
        dag=ftransfer_dag_api_to_bigquery  # Ensure DAG is passed explicitly
    )

    # Define task dependencies
fetch_api_data >> upload_csv_to_gcs >> api_load_to_bigquery