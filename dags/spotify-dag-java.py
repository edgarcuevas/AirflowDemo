from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 1, 1),
}

dag = DAG(
    'run_jar_file',
    default_args=default_args,
    description='An Airflow DAG to run a JAR file',
    schedule_interval='@daily',
)


run_jar_task = BashOperator(
    task_id='run_jar',
    bash_command='java -jar /opt/airflow/jars/musicspotifysearch.jar',
    dag=dag,
)

# Set task dependencies
run_jar_task