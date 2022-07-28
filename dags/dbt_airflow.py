from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['shyakakevin1@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    "start_date": datetime(2022, 7, 20, 2, 30, 00),
    'retry_delay': timedelta(minutes=5)
} 

with DAG(
        dag_id='dbt_airflow',
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
) as dag:
        dbt_pwd = BashOperator(task_id = "dbt_pwd",bash_command="pwd")
        dbt_run = BashOperator(task_id="dbt_run",bash_command="dbt run --profiles-dir /opt/airflow/dbt/data_warehouse --project-dir /opt/airflow/dbt/data_warehouse")
        dbt_test = BashOperator(task_id="dbt_test",bash_command="dbt test --profiles-dir /opt/airflow/dbt/data_warehouse --project-dir /opt/airflow/dbt/data_warehouse")
        dbt_doc_gen = BashOperator(task_id="dbt_doc_gen",bash_command="dbt docs generate --profiles-dir /opt/airflow/dbt/data_warehouse --project-dir /opt/airflow/dbt/data_warehouse")


dbt_pwd >> dbt_run >> dbt_test >> dbt_doc_gen