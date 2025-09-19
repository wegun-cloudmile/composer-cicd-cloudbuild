from __future__ import annotations
from datetime import datetime
import json
import base64
import logging

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubPullOperator,
)
###
PROJECT_ID = "tw-rd-data-wegun-hsia"
TOPIC_ID = "new-orders-for-bq"
SUBSCRIPTION = "new-orders-for-bq-composer-sub"
TARGET_DAG_ID = "pubsub_to_bq_orders"


def _prepare_dag_run_configs_callable(pulled_messages: list, context) -> list[dict]:
    logger = logging.getLogger("airflow.task")
    if not pulled_messages:
        logger.info("No messages pulled from Pub/Sub.")
        return []

    configs = []
    for idx, msg in enumerate(pulled_messages):
        logger.info(f"msg: {msg}")

        message_data_str = msg.message.data
        if isinstance(message_data_str, bytes):
            message_data_str = message_data_str.decode("utf-8")
    
        logger.info(f"message_data_str: {message_data_str}")
        logger.info("="*100)
        
        try:
            message_data_dict = json.loads(message_data_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message {idx} as JSON: {e}")
            continue

        logger.info(f"Parsed message {idx} data: {message_data_dict}")
        configs.append({
            "trigger_dag_id": TARGET_DAG_ID,
            "conf": message_data_dict
        })
    
    logger.info(f"Prepared {len(configs)} DAG run configurations.")
    return configs


with DAG(
    dag_id="trigger_dag_from_pubsub",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["controller", "pubsub", "traditional"],
) as dag:
    # make sure subscription exists
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="ensure_subscription_exists",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        subscription=SUBSCRIPTION,
    )

    # pull messages from pub/sub
    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages",
        project_id=PROJECT_ID,
        ack_messages=True,
        subscription=SUBSCRIPTION,
        max_messages=50,
        messages_callback=_prepare_dag_run_configs_callable,
    )

    # trigger dag task dynamically, .partial().expand_kwargs() 這種寫法在傳統模式下依然是推薦的動態任務語法
    trigger_target_dag = TriggerDagRunOperator.partial(
        task_id="trigger_target_dag"
    ).expand_kwargs(
        XComArg(pull_messages_operator) # get the return value from previous function's output
    )
    ###
    subscribe_task >> pull_messages_operator >> trigger_target_dag