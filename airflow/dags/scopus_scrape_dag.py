from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from scopus_produce import process_directory

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 5, 6, 23, 59),
#     'retries': 0,
# }

# dag = DAG(
#     'nature_scrape_and_process',
#     default_args=default_args,
#     description='Scrape nature.com and process articles',
#     schedule_interval='@daily',
#     catchup=False
# )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 6, 23, 59),  # Set to a past date for immediate execution
    'retries': 0,
}

dag = DAG(
    'scopus_process',
    default_args=default_args,
    description='extract scopus and process json for each year',
    schedule_interval='@once',  # Change to '@once' for a one-time immediate execution
    catchup=False
)

# task = PythonOperator(
#         task_id=f'process_data_{2018}',
#         python_callable=process_directory,
#         op_kwargs={'y': 2018},
#         dag=dag,
# )

years = range(2018, 2024)
for year in years:
    task = PythonOperator(
        task_id=f'process_data_{year}',
        python_callable=process_directory,
        op_kwargs={'year': year},
        dag=dag,
    )