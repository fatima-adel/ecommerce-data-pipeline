from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
import csv
import logging
from google.cloud import storage


default_args = {
    'retries': 1
}

TABLES_TO_TRANSFER = ["orders", "customers", "products", "product_category_name_translation", "order_items", "order_reviews"]  # List of tables
#TABLES_TO_TRANSFER = ["order_reviews"]  # List of tables

table_queries = {
    "orders": "SELECT * FROM orders",
    "customers": "SELECT * FROM customers",  
    "products": "SELECT * FROM products",  
    "product_category_name_translation": "SELECT * FROM  product_category_name_translation",
    "order_items": "SELECT * FROM order_items",
    "order_reviews": "SELECT review_id,	order_id, review_score  FROM order_reviews"
}

with DAG(
    "ftransfer_dag_postgres_database",
    default_args=default_args,
    description="postgres_to_bigquery_transfer",
    start_date=datetime(2024, 4, 20),
    schedule_interval=None
) as ftransfer_dag_postgres_database:
    
    for table_name in TABLES_TO_TRANSFER:
        sql_query = table_queries.get(table_name)  # Get the query from the dictionary

        if sql_query: # Check if query exists
            postgres_to_gcs = PostgresToGCSOperator(
                task_id=f"postgres_to_gcs_{table_name}",
                postgres_conn_id="postgres_conn",
                sql=sql_query,  # Use the retrieved query
                bucket="ready-d25-postgres-to-gcs",
                filename=f"fatima/fatima_{table_name}.csv",
                export_format="csv",
            )
        else:
            print(f"No query found for table: {table_name}")

        load_to_bq = GCSToBigQueryOperator(
            task_id=f"load_to_bq_{table_name}",  # Dynamic task ID
            bucket="ready-d25-postgres-to-gcs",
            source_objects=[f"fatima/fatima_{table_name}.csv"],  # Dynamic source object
            source_format="CSV",
            destination_project_dataset_table=f"ready-de-25.landing.fatima_{table_name}",  # Dynamic table name
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            ignore_unknown_values=True,
        )

    postgres_to_gcs >> load_to_bq

