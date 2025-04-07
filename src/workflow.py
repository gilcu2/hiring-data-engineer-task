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
    description='Updating clickhouse from postgres',
    schedule_interval='0 6 * * *',
    tags=['pipeline'],
) as dag:

    task = PythonOperator(
        task_id='print_message',
        python_callable=pipeline,
    )
