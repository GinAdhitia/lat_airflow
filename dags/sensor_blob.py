from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


AZURE_CONNECTION_ID = 'azure_blob_bvartadata'
CONTAINER_NAME = 'bvarta-internal-data'
DIRECTORY_PATH = 'temp/ingest/'

def check_azure_blob_for_files():
    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    files = hook.get_blobs_list(container_name=CONTAINER_NAME, prefix=DIRECTORY_PATH)

    if files:
        print("Files found in Azure Blob Storage: ok")
    else:
        raise ValueError("No files found in Azure Blob Storage. Retrying...")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'retry_delay': timedelta(minutes=1),
}


with DAG('azure_blob_check', default_args=default_args, schedule_interval='* * * * *') as dag:
    check_files_task = PythonOperator(
        task_id='check_azure_blob_files',
        python_callable=check_azure_blob_for_files,
    )

    check_files_task
