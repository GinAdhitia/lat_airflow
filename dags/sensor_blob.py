from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
import time


AZURE_CONNECTION_ID = 'azure_blob_bvartadata'
SOURCE_CONTAINER_NAME = 'bvarta-internal-data'
SOURCE_DIRECTORY_PATH = 'temp/ingest/'
TARGET_CONTAINER_NAME = 'bvarta-internal-data'
TARGET_DIRECTORY_PATH = 'temp/stg/'


def check_azure_blob_for_files():
    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    files = hook.get_blobs_list(container_name=SOURCE_CONTAINER_NAME, prefix=SOURCE_DIRECTORY_PATH)

    if files:
        print("Files found in Azure Blob Storage: ok")
        for file in files:
            source_blob_name = file
            target_blob_name = file.replace(SOURCE_DIRECTORY_PATH, TARGET_DIRECTORY_PATH)

            source_blob_client = hook.get_conn().get_blob_client(container=SOURCE_CONTAINER_NAME, blob=source_blob_name)
            target_blob_client = hook.get_conn().get_blob_client(container=TARGET_CONTAINER_NAME, blob=target_blob_name)
            
            target_blob_client.start_copy_from_url(source_blob_client.url)
            
            while True:
                copy_props = target_blob_client.get_blob_properties()
                if copy_props.copy.status == 'success':
                    break
                elif copy_props.copy.status == 'failed':
                    raise Exception(f"Copy failed for blob: {source_blob_name}")
                time.sleep(3)
            print(f"File {file} moved")

            # hook.delete_blobs(container_name=SOURCE_CONTAINER_NAME, blob_name=source_blob_name)
            hook.delete_file(container_name=SOURCE_CONTAINER_NAME, blob_name=source_blob_name)
            # hook.delete_blob(container_name=SOURCE_CONTAINER_NAME, blob_name=source_blob_name)
            
            
            print(f"File {file} Deleted")
    else:
        raise ValueError("No files found in Azure Blob Storage. Retrying...")


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
