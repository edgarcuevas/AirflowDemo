from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the Airflow DAG
dag = DAG(
    's3_to_spotify',
    default_args=default_args,
    schedule_interval='@daily',
)

# Function to check for JSON files in a specific S3 prefix
def check_s3_prefix(prefix, bucket_name='spotify-requests', aws_conn_id='aws_local'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    json_files = [key for key in keys if key.endswith('.json')]
    return bool(json_files)

# Define the task to wait for JSON files in the S3 prefix
def wait_for_s3_prefix(prefix, **context):
    print(f"Waiting for JSON files in S3 prefix: {prefix}")
    s3_hook = S3Hook(aws_conn_id='aws_local')
    while not check_s3_prefix(prefix):
        print("No JSON files found yet. Waiting...")
        # Sleep for a few seconds before checking again
        time.sleep(10)
    print("JSON files found in S3 prefix. Proceeding...")

# Define the task to read the S3 file
def read_s3_file(prefix, **context):
    print(f"Reading JSON file from S3 prefix: {prefix}")
    s3_hook = S3Hook(aws_conn_id='aws_local')
    keys = s3_hook.list_keys(bucket_name='spotify-requests', prefix=prefix)
    json_key = [key for key in keys if key.endswith('.json')][0]  # Assume first JSON file found
    file_content = s3_hook.get_key(key=json_key, bucket_name='spotify-requests').get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    song_name = json_content['song_name']
    print(f"Extracted song name: {song_name}")
    return song_name

# Define the task to wait for S3 prefix
wait_for_s3_prefix_task = PythonOperator(
    task_id='wait_for_s3_prefix',
    python_callable=wait_for_s3_prefix,
    op_kwargs={'prefix': 'songs/'},
    dag=dag,
)

# Define the task to execute the read_s3_file function
read_s3_file_task = PythonOperator(
    task_id='read_s3_file',
    python_callable=read_s3_file,
    op_kwargs={'prefix': 'songs/'},
    provide_context=True,  # Pass Airflow context to the function
    dag=dag,
)

# Set task dependencies
wait_for_s3_prefix_task >> read_s3_file_task
