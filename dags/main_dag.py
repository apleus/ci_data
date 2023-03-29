from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

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
    extract_raw_data = BashOperator(
        task_id='extract1',
        bash_command='/opt/airflow/tasks/extract_raw_data.py'
    )
    extract_prep_data = BashOperator(
        task_id='extract2',
        bash_command='/opt/airflow/tasks/extract_prep_data.py'
    )
    load_prep_data = BashOperator(
        task_id='load1',
        bash_command='/opt/airflow/tasks/load_prep_data.py'
    )

(
    extract_raw_data
    >> extract_prep_data
    >> load_prep_data
)