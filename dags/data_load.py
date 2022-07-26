import airflow
import pandas as pd 
from datetime import timedelta,datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

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

def insert_data():

    postgres_hook  = PostgresHook(postgres_conn_id="postgres_local")
    engine  = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_csv("/opt/airflow/data/20181024_d1_0830_0900.csv", sep='[,;:]', index_col=False)
    new_columns =['track_id','type','traveled_d','avg_speed','lat','lon','speed','lon_acc','lat_acc','time']
    df.columns = new_columns

    df.to_sql("traffic_table_",con=engine,if_exists="replace",index_label='id')
    print('completed')

with DAG(
        dag_id='data_load',
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
) as dag:
     task1 = PostgresOperator(
        task_id="table_creation",
        postgres_conn_id="postgres_local",
        sql="""
            create table if not exists traffic_table_ (track_id INT NOT NULL,type TEXT DEFAULT NULL,traveled_d FLOAT DEFAULT NULL,
                                                     avg_speed FLOAT DEFAULT NULL, lat FLOAT DEFAULT NULL,lon FLOAT DEFAULT NULL,
                                                     speed FLOAT DEFAULT NULL,lon_acc FLOAT DEFAULT NULL,lat_acc FLOAT DEFAULT NULL,
                                                     time FLOAT NULL DEFAULT NULL,primary key (track_id));

     """,
     )

     task2 = PythonOperator(
        task_id="loading_data",
        python_callable=insert_data)

task1 >> task2