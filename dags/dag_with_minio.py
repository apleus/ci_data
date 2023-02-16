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
conn_id = 'minio_conn'

# def scrape_product_reviews():
    
#     s3_hook = S3Hook(aws_conn_id='minio_conn')
#     s3_hook.load_file(
#         filename=f"dags/get_orders_{ds_nodash}.txt",
#         key=f"orders/{ds_nodash}.txt",
#         bucket_name='products',
#         replace=True
#         )

with DAG (
    default_args = default_args,
    dag_id = 'main_dag',
    start_date = datetime(2023, 2, 1),
    schedule_interval = '@daily'
) as dag:
    scrape_product_reviews = BashOperator(
        task_id='scrape_product_reviews',
        bash_command='/opt/airflow/tasks/scrape_product_reviews.py'
    )
    # task1 = S3KeySensor(
    #     task_id = 'sensor_s3',
    #     bucket_name = 'airflow',
    #     bucket_key = 'data.csv',
    #     aws_conn_id = conn_id,
    #     mode='poke',
    #     poke_interval=5,
    #     timeout=30
    # )