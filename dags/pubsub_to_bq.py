from __future__ import annotations
from datetime import datetime
import json
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- Config ---
GCP_PROJECT_ID = "tw-rd-data-wegun-hsia"
BQ_DATASET = "test_dataset"
BQ_TABLE = "orders_raw"


def _process_and_insert_into_bq_callable(**context):
    """
    """
    logger = logging.getLogger("airflow.task")
    dag_run_conf = context["dag_run"].conf
    logger.info(f"Received configuration: {dag_run_conf}")

    if not dag_run_conf or not isinstance(dag_run_conf, dict):
        logger.warning(f"DAG run configuration is empty or not a dictionary. Skipping. Conf: {dag_run_conf}")
        return

    order_data = dag_run_conf

    rows_to_insert = [{
        "order_id": order_data.get("orderId"),
        "order_timestamp": order_data.get("timestamp"),
        "customer_id": order_data.get("customerId"),
        "order_items": json.dumps(order_data.get("items", []), ensure_ascii=False),
        "total_price": sum(
            item.get("unit_price", 0) * item.get("quantity", 0)
            for item in order_data.get("items", [])
        ),
        "order_status": "PENDING",
        "processing_timestamp": datetime.now().isoformat(),
    }]

    logger.info(f"Preparing to insert rows into BigQuery: {rows_to_insert}")

    # insert to BigQuery using BigQueryHook
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client(project_id=GCP_PROJECT_ID)

    errors = client.insert_rows_json(
        table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        json_rows=rows_to_insert,
    )

    # 4. 檢查是否有寫入錯誤
    if errors:
        logger.error("Errors occurred while inserting rows into BigQuery.")
        raise ValueError(f"BigQuery insert failed: {errors}")
    else:
        logger.info(f"Successfully inserted {len(rows_to_insert)} row(s) into BigQuery table {BQ_TABLE}.")


with DAG(
    dag_id="pubsub_to_bq_orders",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["worker", "bigquery", "hook"],
) as dag:
    process_and_insert_task = PythonOperator(
        task_id="process_and_insert_task",
        python_callable=_process_and_insert_into_bq_callable,
    )