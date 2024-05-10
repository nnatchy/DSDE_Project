from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Import your specific functions
from extract_zip import download_files
from nature_scrape_dag import scrape_and_save, process_article_details, send_nature_to_kafka
from scopus_produce import process_directory
from nature_consume import receive_nature_from_kafka
from scopus_consume import receive_scopus_from_kafka
from nature_spark import process_nature_dataset
from scopus_spark import process_scopus_dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6, 23, 59),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_pipeline_till_spark',
    default_args=default_args,
    description="A pipeline to handle web scraping and processing",
    schedule_interval='@daily',
    catchup=False
)

# Define tasks
download_task = PythonOperator(
    task_id='download_zip_files',
    python_callable=download_files,
    dag=dag,
)

scrape_task = PythonOperator(
    task_id='scrape_nature',
    python_callable=scrape_and_save,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_article_details',
    python_callable=process_article_details,
    dag=dag,
)

send_to_kafka_task = PythonOperator(
    task_id='send_nature_to_kafka',
    python_callable=send_nature_to_kafka,
    dag=dag,
)

receive_from_kafka_task = PythonOperator(
    task_id='receive_nature_from_kafka',
    python_callable=receive_nature_from_kafka,
    dag=dag,
)

nature_spark_job = SparkSubmitOperator(
    task_id=f'nature_spark_job',
    conn_id="spark-conn",
    application="dags/nature_spark.py",
    dag=dag
)

# Set dependencies to ensure proper sequence
download_task >> scrape_task >> process_task >> [send_to_kafka_task, receive_from_kafka_task] >> nature_spark_job  # Direct sequence

# Handling each year
prod_l = []
cons_l = []
years = range(2018, 2024)
for year in years:
    produce_task = PythonOperator(
        task_id=f'process_data_{year}',
        python_callable=process_directory,
        op_kwargs={'year': year},
        dag=dag,
    )
    consume_task = PythonOperator(
        task_id=f'consume_scopus_kafka_{year}',
        python_callable=receive_scopus_from_kafka,
        op_kwargs={'year': year},
        dag=dag,
    )
    scopus_spark_job = SparkSubmitOperator(
        task_id=f'scopus_spark_job_{year}',
        conn_id="spark-conn",
        application="dags/scopus_spark.py",
        application_args=[str(year)],
        dag=dag
    )
    prod_l.append(produce_task)
    cons_l.append(consume_task)
    process_task >> [produce_task, consume_task] >> scopus_spark_job # Ensure sequence for each year
