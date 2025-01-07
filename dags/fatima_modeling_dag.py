from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

with DAG(
    dag_id="fbigquery_modeling_transformation",
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    tags=["fatima","bigquery", "modeling", "transformation"],
) as fbigquery_modeling_transformation:
    
    source_table = "`ready-de-25.landing`"

    transformations = {
        "view_fact_orders": 
            f""" CREATE OR REPLACE VIEW `{source_table}.fview_fact_orders` AS
            SELECT DISTINCT o.order_id, o.customer_id, o.order_status, o.order_delivered_customer_date,
                o.oder_estimated_delivery_date, p.payment_type, CASE
                    WHEN p.payment_value < 0 THEN 0
                    ELSE p.payment_value
                    END AS payment_value, r.review_score
            FROM `ready-de-25.landing.fatima_orders` o
            LEFT JOIN `ready-de-25.landing.fatima_order_review` r on o.order_id = r.order_id
            LEFT JOIN `ready-de-25.landing.fatima_order_payments` p on o.order_id = p.order_id """,

        "view_dim_customer": 
            f""" CREATE OR REPLACE VIEW `{source_table}.fview_dim_customer` AS 
            SELECT DISTINCT customer_id, customer_city, customer_state 
            FROM `ready-de-25.landing.fatima_customers`""",

        "view_dim_product": 
            f"""CREATE OR REPLACE VIEW `{source_table}.fview_dim_product` AS
            SELECT DISTINCT oi.order_id, p.product_id, e.product_category_name, CASE
        WHEN p.product_weight_g < 0 THEN 0
        ELSE p.product_weight_g
        END AS product_weight_g
        FROM ready-de-25.landing.fatima_products` p
        LEFT JOIN `ready-de-25.landing.fatima_product_category_name_translation` e on  p.product_category_name = e.product_category_name
        LEFT JOIN `ready-de-25.landing.fatima_order_items` oi on p.product_id = oi.product_id """,

        "view_dim_order_items": 
            f""" CREATE OR REPLACE VIEW `{source_table}.fview_dim_order_items` AS
           SELECT DISTINCT order_id, order_item, product_id, seller_id, price
            FROM `ready-de-25.landing.fatima_order_items` """,
    }

    for view_name, sql_query in transformations.items():
        create_view_task = BigQueryExecuteQueryOperator(
            task_id=f"create_{view_name}",
            sql=sql_query,
            use_legacy_sql=False,  # Use standard SQL
        )