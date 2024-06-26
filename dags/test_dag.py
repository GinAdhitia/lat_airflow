from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 26),
    'retries': 1
}

# Instantiate the DAG object
dag = DAG(
    'test_dag1',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='* * * * *',
)

# Define tasks
task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)

def print_hello():
    return 'Hello Airflow!'

task2 = PythonOperator(
    task_id='task2',
    python_callable=print_hello,
    dag=dag,
)

task1 >> task2
