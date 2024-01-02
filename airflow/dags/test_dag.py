from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'diaz',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def run_task(task_id, bash_command, **kwargs):
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=120),
        **kwargs
    )

with DAG(
    default_args=default_args,
    dag_id='tes_poi_indonesia',  # Changed to use underscores instead of spaces
    description='DAG for testing 2 store',
    start_date=datetime(2023, 12, 7, tzinfo=local_tz),
    schedule_interval='@hourly',  # Run every hour
    catchup=False
) as dag:
  
    tasks = [
        {'task_id': 'kfc', 'bash_command': 'python3 /root/airflow/poi-indonesia/poi-indonesia/kfc.py'},
        {'task_id': 'uniqlo', 'bash_command': 'python3 /root/airflow/poi-indonesia/poi-indonesia/uniqlo.py'},
        {'task_id': 'copy_to_snowflake_table', 'bash_command': 'python3 /root/airflow/poi-indonesia/poi-indonesia/copy_to_snowflake_table.py'}
        # Add more tasks as needed
    ]


    for task_info in tasks:
        task = run_task(**task_info)
        
        if 'prev_task' in locals():
            prev_task >> task
        
        prev_task = task


