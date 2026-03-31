# pubg_pipeline_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pubg_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
