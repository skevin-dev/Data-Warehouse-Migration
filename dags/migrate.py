import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator





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

def migrate():
    src = PostgresHook(postgres_conn_id="postgres_local")
    target= MySqlHook(mysql_conn_id="mysql_local")
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    target_conn= target.get_conn()
    target_cursor = target_conn.cursor()

    cursor.execute("SELECT * FROM traffic_table_")

    target.insert_rows(table="traffic_table",rows=cursor.fetchall(),replace=True,replace_index="id")


with DAG(
        dag_id='migrate_data',
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
) as dag:

      create_table_mysql = MySqlOperator(
        task_id="create_mysql_table",
        mysql_conn_id="mysql_local",
        sql="""
           use airflow_mysql; create table if not exists traffic_table (id int primary key,track_id int,type text,traveled_d double,avg_speed double,
                                                     lat double,lon double,speed double,lon_acc double,lat_acc double,time double)
        """,
    )

      migrate_op = PythonOperator(task_id="migrate_data",
        python_callable=migrate)


create_table_mysql >> migrate_op 