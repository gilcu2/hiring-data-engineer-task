from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def pipeline():
    print("Updating clickhouse from postgres")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='pipeline_task',
    default_args=default_args,
    description='A simple periodic task',
    schedule_interval='@daily',  # ⏰ Run once per day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task = PythonOperator(
        task_id='print_message',
        python_callable=pipeline,
    )
