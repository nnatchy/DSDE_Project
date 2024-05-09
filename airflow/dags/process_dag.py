from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from scopus_consume import receive_scopus_from_kafka
from nature_consume import receive_nature_from_kafka

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6, 23, 58),
    'retries': 0,
}

dag = DAG(
    'kafka_to_spark_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

nature_task = PythonOperator(
    task_id=f'consume_nature_kafka',
    python_callable=receive_nature_from_kafka,
    dag=dag,
)

years = range(2018, 2024)
for year in years:
    consume_task = PythonOperator(
        task_id=f'consume_scopus_kafka_{year}',
        python_callable=receive_scopus_from_kafka,
        op_kwargs={'year': year},
        dag=dag,
    )
