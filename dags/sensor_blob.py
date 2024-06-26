from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import time
import json
import pandas as pd
from io import BytesIO


AZURE_CONNECTION_ID   = 'azure_blob_bvartadata'
SOURCE_CONTAINER_NAME = 'bvarta-internal-data'
SOURCE_DIRECTORY_PATH = 'temp/ingest/'
TARGET_CONTAINER_NAME = 'bvarta-internal-data'
TARGET_DIRECTORY_PATH = 'temp/stg/'


# Util
def transform(blob_client, file):
    blob_data = BytesIO()
    blob_client.download_blob().readinto(blob_data)
    blob_data.seek(0)
    json_content = json.load(blob_data)

    print(f"JSON content of {file}: {json_content}")

    df = pd.DataFrame(json_content)
    df[['node', 'parameter', 'order', 'value']] = df['data'].apply(lambda x: pd.Series([x['node'], x['parameter'], x['order'], x['value']]))
    df = df.drop(columns=['data', 'title', 'description', 'trigger', 'parameter', 'event', 'node'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    df['id_store'] = 1
    df = df.rename(columns={'order': 'refrigerator_no', 'value': 'temperature'})

    print(df.head())
    return df


def load(df):
    # load to postgres
    pass


# DAG
def check_azure_blob_for_files(**kwargs):
    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    files = hook.get_blobs_list(container_name=SOURCE_CONTAINER_NAME, prefix=SOURCE_DIRECTORY_PATH)

    if files:
        print("Files found in Azure Blob Storage: ok")
        kwargs['ti'].xcom_push(key='files_to_process', value=files)
        return 'move_and_process_files'
    else:
        print("No files found in Azure Blob Storage.")
        kwargs['ti'].xcom_push(key='files_to_process', value=[])
        return 'finish'


def move_and_process_files(**kwargs):
    files_to_process = kwargs['ti'].xcom_pull(key='files_to_process', task_ids='check_azure_blob_files')

    if not files_to_process:
        print("No files to process.")
        return

    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)

    for file in files_to_process:
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
        print(f"File {file} Moved")

        df = transform(target_blob_client, file)
        print(f"File {file} Finished Transform")

        load(df)
        print(f"File {file} Loaded to Postgres")

        # hook.delete_file(container_name=SOURCE_CONTAINER_NAME, blob_name=source_blob_name)            
        print(f"File {file} Deleted")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retry_delay': timedelta(minutes=1),
}


with DAG('azure_blob_check', default_args=default_args, schedule_interval='0 0 1 * *') as dag:
    check_files_task = BranchPythonOperator(
        task_id='check_azure_blob_files',
        python_callable=check_azure_blob_for_files,
        provide_context=True,
    )

    move_and_process_files_task = PythonOperator(
        task_id='move_and_process_files',
        python_callable=move_and_process_files,
        provide_context=True,
    )

    finish_task = DummyOperator(
        task_id='finish'
    )

    check_files_task >> [move_and_process_files_task, finish_task]
    move_and_process_files_task >> finish_task
