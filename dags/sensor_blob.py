import json
import logging
import pandas as pd
import time
from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from io import BytesIO

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime.now(),
}

dag = DAG(
    'azure_blob_check',
    default_args=default_args,
    schedule_interval='* * * * *'
)


AZURE_CONNECTION_ID    = 'azure_blob_bvartadata'
POSTGRES_CONNECTION_ID = 'dbwarehouse'
SOURCE_CONTAINER_NAME  = 'bvarta-internal-data'
SOURCE_DIRECTORY_PATH  = 'temp/ingest/'
TARGET_CONTAINER_NAME  = 'bvarta-internal-data'
TARGET_DIRECTORY_PATH  = 'temp/stg/'


# ETL
def transform(blob_client):
    blob_data = BytesIO()
    blob_client.download_blob().readinto(blob_data)
    blob_data.seek(0)
    
    try:
        json_content = json.load(blob_data)
    except json.JSONDecodeError:
        raise ValueError(f"Failed to load JSON content")
    
    df = pd.DataFrame(json_content)
    df[['node', 'parameter', 'order', 'value']] = df['data'].apply(lambda x: pd.Series([x['node'], x['parameter'], x['order'], x['value']]))
    df = df.drop(columns=['data', 'title', 'description', 'trigger', 'parameter', 'event', 'node'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    df['id_store'] = 1
    df = df.rename(columns={'order': 'refrigerator_no', 'value': 'temperature'})

    return df


def load(df):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql('logs', engine, if_exists='append', index=False)
    return None


# Util
def check_azure_blob_for_files(**kwargs):
    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    files = hook.get_blobs_list(container_name=SOURCE_CONTAINER_NAME, prefix=SOURCE_DIRECTORY_PATH)
    
    if files:
        kwargs['ti'].xcom_push(key='files_to_process', value=files)
        return 'process_files'
    else:
        kwargs['ti'].xcom_push(key='files_to_process', value=[])
        return 'finish'


def process_files(**kwargs):
    files_to_process = kwargs['ti'].xcom_pull(key='files_to_process', task_ids='check_azure_blob_files')
    
    if not files_to_process:
        return
    
    hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    
    for file in files_to_process:
        logging.info(f'--- File {file} ---')
        source_blob_name = file
        target_blob_name = file.replace(SOURCE_DIRECTORY_PATH, TARGET_DIRECTORY_PATH)
        
        source_blob_client = hook.get_conn().get_blob_client(container=SOURCE_CONTAINER_NAME, blob=source_blob_name)
        target_blob_client = hook.get_conn().get_blob_client(container=TARGET_CONTAINER_NAME, blob=target_blob_name)
        
        target_blob_client.start_copy_from_url(source_blob_client.url)
        logging.info('Finished Moved File')
        
        while True:
            copy_props = target_blob_client.get_blob_properties()
            if copy_props.copy.status == 'success':
                break
            elif copy_props.copy.status == 'failed':
                raise Exception(f"Copy failed for blob: {source_blob_name}")
            time.sleep(3)
        
        df = transform(target_blob_client)
        logging.info('Finished Transform')

        load(df)
        logging.info('Finished Load')
        
        hook.delete_file(container_name=SOURCE_CONTAINER_NAME, blob_name=source_blob_name)
        logging.info('Finished Delete')
    
    return 'finish'


# Define Task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)
    
check_files_task = BranchPythonOperator(
    task_id='check_azure_blob_files',
    python_callable=check_azure_blob_for_files,
    provide_context=True,
    dag=dag,
)

move_and_process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)

finish_task = DummyOperator(
    task_id='finish',
    dag=dag,
)

start_task >> check_files_task >> [move_and_process_files_task, finish_task]
move_and_process_files_task >> finish_task
