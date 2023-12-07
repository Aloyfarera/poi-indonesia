from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.snowflake_operator import SnowflakeOperator
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
        # Add more tasks as needed
    ]

    # Snowflake task
    snowflake_task = SnowflakeOperator(
        task_id='copy_into_task',
        sql="COPY INTO POI_INDONESIA.PUBLIC.POI_INDONESIA_MASTER \
            FROM @STG_SI_DEMO_STAGE_1 \
            FILE_FORMAT = csv1 \
            PATTERN = '.*csv' \
            FORCE = FALSE",
        warehouse='COMPUTE_WH',
        database='POI_INDONESIA',
        role='ACCOUNTADMIN',
        schema='PUBLIC',
    )

    for task_info in tasks:
        task = run_task(**task_info)
        
        if 'prev_task' in locals():
            prev_task >> task
        
        prev_task = task

    # Link Snowflake task to the last Python task
    prev_task >> snowflake_task
