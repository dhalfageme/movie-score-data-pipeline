from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.bronze import provider1 as br1

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "bronze_provider1",
    default_args=default_args,
    start_date=datetime(2026,3,23),
    schedule_interval="@weekly",
    catchup=False,
)

bronze_task = PythonOperator(
    task_id="bronze_provider1",
    python_callable=br1.bronze_provider1,
    dag=dag
)