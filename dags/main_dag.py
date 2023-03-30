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
    start_date = datetime(2023, 3, 28),
    schedule_interval = '@daily'
) as dag:
    extract_raw_data = BashOperator(
        task_id='extract1',
        bash_command='python3 /opt/airflow/tasks/extract_raw_data.py',
        depends_on_past = True
    )
    extract_prep_data = BashOperator(
        task_id='extract2',
        bash_command='python3 /opt/airflow/tasks/extract_prep_data.py',
        depends_on_past = True
    )
    load_prep_data = BashOperator(
        task_id='load1',
        bash_command='python3 /opt/airflow/tasks/load_prep_data.py',
        depends_on_past = True
    )
    create_dbt_profile = BashOperator(
        task_id='transform1',
        bash_command="python3 /opt/airflow/tasks/transform_dbt/profile.py",
        depends_on_past = True
    )
    transform_dbt = DbtRunOperator(
        task_id="transform2",
        dir="/opt/airflow/tasks/transform_dbt/",
        profiles_dir="/opt/airflow/tasks/transform_dbt",
        depends_on_past = True
    )
    test_dbt = DbtTestOperator(
        task_id="transform3",
        dir="/opt/airflow/tasks/transform_dbt/",
        profiles_dir="/opt/airflow/tasks/transform_dbt",
        depends_on_past = True
    )

(
    extract_raw_data
    >> extract_prep_data
    >> load_prep_data
    >> create_dbt_profile
    >> transform_dbt
    >> test_dbt
)