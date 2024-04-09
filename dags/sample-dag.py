from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
from botocore.exceptions import EndpointConnectionError

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_s3_file():
    print("Made it here to check_s3_file function")
    s3_hook = S3Hook(aws_conn_id='aws_local')
    bucket_name = 'spotify-requests'
    try:
        keys = s3_hook.list_keys(bucket_name)
    except EndpointConnectionError as e:
        
        print(f"Error: {e}")
        keys = None
    if keys:
        for key in keys:
            print(f"File {key} exists in bucket {bucket_name}")
    else:
        print(f"No files exist in bucket {bucket_name}")

# Define the DAG in the global scope
dag = DAG('s3_to_spotify_sample', default_args=default_args, schedule_interval='@daily')

s3_file_sensor_task = PythonOperator(
    task_id='s3_file_sensor',
    python_callable=check_s3_file,
    dag=dag,  # Associate the task with the DAG
)

s3_file_sensor_task