from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'apleus',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args = default_args,
    dag_id = 'main_dag',
    start_date = datetime(2023, 14, 1),
    schedule_interval = '@daily'
) as dag:
    scrape_product_reviews = BashOperator(
        task_id='scrape_product_reviews',
        bash_command='/opt/airflow/tasks/scrape_product_reviews.py'
    )
    scrape_product_reviews