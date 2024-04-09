"""
Written by Edgar as a demo
"""

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_s3_file(task_instance):
    print("Made it here to read_s3_file function")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print(s3_hook)
    # Fetch the matched key from XCom
    matched_key = task_instance.xcom_pull(task_ids='s3_file_sensor', key='return_value')
    content_object = s3_hook.get_key(matched_key, 'spotify-requests')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content['song_name']

def check_spotify(song_name):
    print("Adding song to Spotify: ", song_name)

dag = DAG(
    's3_to_spotify',
    default_args=default_args,
    description='DAG to detect new file in S3 bucket and check song on Spotify',
    schedule_interval=timedelta(minutes=5),
)

s3_file_sensor = S3KeySensor(
    task_id='s3_file_sensor',
    bucket_key='s3://spotify-requests/songs/*.json',
    wildcard_match=True,
    aws_conn_id='aws_default',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag,
)

read_s3_file_task = PythonOperator(
    task_id='read_s3_file',
    python_callable=read_s3_file,
    provide_context=True,  # This is necessary to access task_instance
    dag=dag,
)

check_spotify_task = PythonOperator(
    task_id='check_spotify',
    python_callable=check_spotify,
    provide_context=True,
    dag=dag,
)

s3_file_sensor >> read_s3_file_task >> check_spotify_task