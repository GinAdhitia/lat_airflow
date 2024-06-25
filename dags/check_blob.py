from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

def read_blob_to_dataframe(**kwargs):
    account_name = 'bvartadata'
    # account_key = '303rYYBPxOK50+vgM3bW8cWaEkVg2fy549ISJbnvtI6l5lVT1W2uNhGMuzP9HEmY9B4Tzl8Bqj+6+AStfczt4A=='
    container_name = 'bvarta-internal-data'
    blob_name = 'temp/sample.csv'
    
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob().content_as_text()
    df = pd.read_csv(StringIO(blob_data))
    print(df)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='check_blob',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    check_blob = WasbBlobSensor(
        task_id='check_blob_exists',
        wasb_conn_id='azure_blob_bvartadata',
        container_name='bvarta-internal-data',
        blob_name='temp/sample.csv',
        poke_interval=60,
        timeout=60*5
    )

    print_blob_df = PythonOperator(
        task_id='print_blob_df',
        python_callable=read_blob_to_dataframe,
        provide_context=True,
        dag=dag,
    )

    check_blob >> print_blob_df
