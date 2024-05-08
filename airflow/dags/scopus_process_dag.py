from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from scopus_consume import receive_from_kafka

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_to_spark_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

years = range(2018, 2024)
for year in years:
    # consume_task = PythonOperator(
    #     task_id=f'consume_kafka_messages_{year}',
    #     python_callable=receive_from_kafka,
    #     op_kwargs={'year': year},
    #     dag=dag,
    # )

    spark_task = SparkSubmitOperator(
        task_id=f'process_with_spark_{year}',
        conn_id='spark_conn',
        application="dags/scopus_spark.py",
        application_args=[str(year)],
        dag=dag,
    )

    # consume_task >> spark_task 
    # spark_task
    # pass
    
# import airflow
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# dag = DAG(
#     dag_id = "sparking_flow",
#     default_args = {
#         "owner": "Yusuf Ganiyu",
#         "start_date": airflow.utils.dates.days_ago(1)
#     },
#     schedule_interval = "@daily"
# )

# start = PythonOperator(
#     task_id="start",
#     python_callable = lambda: print("Jobs started"),
#     dag=dag
# )

# python_job = SparkSubmitOperator(
#     task_id="python_job",
#     conn_id="spark-conn",
#     application="dags/scopus_spark.py",
#     dag=dag
# )

# end = PythonOperator(
#     task_id="end",
#     python_callable = lambda: print("Jobs completed successfully"),
#     dag=dag
# )

# start >> python_job >> end