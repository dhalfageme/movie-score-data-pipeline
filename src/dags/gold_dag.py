# airflow/dags/gold_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from src.silver.intermediate import movies_unified
from src.gold import movies_gold_incremental

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "gold_movies",
    default_args=default_args,
    start_date=datetime(2026,3,23),
    schedule_interval="@daily",
    catchup=False,
)

wait_silver = ExternalTaskSensor(
    task_id="wait_silver",
    external_dag_id="silver_intermediate",
    external_task_id="silver_intermediate_build",
    timeout=600,
    poke_interval=60,
    mode="poke",
    dag=dag
)

gold_task = PythonOperator(
    task_id="gold_incremental_build",
    python_callable=movies_gold_incremental.build_gold_incremental,
    dag=dag
)

wait_silver >> gold_task