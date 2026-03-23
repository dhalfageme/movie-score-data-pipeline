# airflow/dags/bronze_provider3_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.bronze import provider3 as br3

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "bronze_provider3",
    default_args=default_args,
    start_date=datetime(2026,3,23),
    schedule_interval="@monthly",
    catchup=False,
)

bronze_task = PythonOperator(
    task_id="bronze_provider3",
    python_callable=br3.bronze_provider3,
    dag=dag
)