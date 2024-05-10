from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Yusuf Ganiyu",
        "start_date": datetime(2023, 5, 10),
        "retries": 1
    },
    schedule_interval="@daily",
    catchup=False
)

nature_job = SparkSubmitOperator(
        task_id=f'nature_spark_job',
        conn_id="spark-conn",
        application="dags/nature_spark.py",
        dag=dag
    )


for year in range(2018, 2024):
    scopus_job = SparkSubmitOperator(
        task_id=f'scopus_spark_job_{year}',
        conn_id="spark-conn",
        application="dags/scopus_spark.py",
        application_args=[str(year)],
        dag=dag
    )
