# airflow/dags/silver_intermediate_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from src.silver.intermediate import movies_unified

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "silver_intermediate",
    default_args=default_args,
    start_date=datetime(2026,3,23),
    schedule_interval="@daily",
    catchup=False,
)

wait_provider1 = ExternalTaskSensor(
    task_id="wait_provider1",
    external_dag_id="bronze_provider1",
    external_task_id="bronze_provider1",
    timeout=600,
    poke_interval=60,
    mode="poke",
    dag=dag
)

wait_provider2 = ExternalTaskSensor(
    task_id="wait_provider2",
    external_dag_id="bronze_provider2",
    external_task_id="bronze_provider2",
    timeout=600,
    poke_interval=60,
    mode="poke",
    dag=dag
)

wait_provider3 = ExternalTaskSensor(
    task_id="wait_provider3",
    external_dag_id="bronze_provider3",
    external_task_id="bronze_provider3",
    timeout=600,
    poke_interval=60,
    mode="poke",
    dag=dag
)

silver_task = PythonOperator(
    task_id="silver_intermediate_build",
    python_callable=movies_unified.build_intermediate_unified,
    dag=dag
)

[wait_provider1, wait_provider2, wait_provider3] >> silver_task