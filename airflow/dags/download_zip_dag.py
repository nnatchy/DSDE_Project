from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from extract_zip import download_files

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
}

dag = DAG(
    'download_and_process_zip_files',
    default_args=default_args,
    description='Download and extract zip files from GitHub',
    schedule_interval='@monthly',
    catchup=False
)

download_task = PythonOperator(
    task_id='download_zip_files',
    python_callable=download_files,
    dag=dag,
)

download_task
