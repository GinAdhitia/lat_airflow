from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


def extract_data_from_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')

    sql = "SELECT * FROM ab_permission"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    print(df)



default_args = {
    'owner' : 'airflow',
    'start_date': datetime(2023,1,1),
    'retries': 1
}

dag = DAG(
    'extract_data_from_postgres',
    default_args = default_args,
    description = "A simple dag to extract data from postgres",
    schedule_interval='@daily',
    max_active_runs = 1,
    catchup = False
)


extract_task = PythonOperator( 
    task_id = 'extract_data',
    python_callable = extract_data_from_postgres,
    dag=dag

)

extract_task 
