# airflow/dags/bronze_provider2_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.bronze import provider2 as br2

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "bronze_provider2",
    default_args=default_args,
    start_date=datetime(2026,3,23),
    schedule_interval="0 0 */15 * *",  # cada 15 días
    catchup=False,
)

bronze_task = PythonOperator(
    task_id="bronze_provider2",
    python_callable=br2.bronze_provider2,
    dag=dag
)