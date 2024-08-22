from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage

from multithread_download import multithread_download
from utils import csv_to_gzip, ndjson_to_csv, check_bucket_existence

default_args = {
    'owner': 'npt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_task',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False, 
)

url = 'http://fake_api:5000/download_ndjson'
connection_id =  'gcp-conn-id'

BUCKET_NAME = 'test_bucket_120301'
PROJECT_ID = 'healthy-life-433312-j8'
LOCATION = 'ASIA-SOUTHEAST1'
DATASET_ID = 'TMP'
TABLE_NAME = 'tmp_table'

start = EmptyOperator(task_id='start', dag=dag)

download_task = PythonOperator(
    task_id='download_task',
    python_callable=multithread_download,
    dag=dag,
    provide_context=True,
    op_kwargs={
      'url': url,
    }
)

ndjson_to_csv_task = PythonOperator(
    task_id='ndjson_to_csv_task',
    python_callable=ndjson_to_csv,
    dag=dag,
    provide_context=True
)

csv_to_gzip_task = PythonOperator(
    task_id='csv_to_gzip_task',
    python_callable=csv_to_gzip,
    dag=dag,
    provide_context=True
)

check_bucket_exist_task = PythonOperator(
    task_id='check_bucket_existence',
    python_callable=check_bucket_existence,
    op_kwargs={
      'bucket_name': BUCKET_NAME,
    }
)

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=BUCKET_NAME,
    location=LOCATION,
    gcp_conn_id=connection_id,
    trigger_rule=TriggerRule.ALL_FAILED,
)

upload_task = LocalFilesystemToGCSOperator(
    task_id='upload_file_task',
    src="{{ task_instance.xcom_pull(task_ids='csv_to_gzip_task', key='file_path') }}",
    dst='/tmp/' + "{{ task_instance.xcom_pull(task_ids='csv_to_gzip_task', key='file_name') }}",
    bucket=BUCKET_NAME,
    gcp_conn_id=connection_id,
    trigger_rule=TriggerRule.ONE_SUCCESS,
)

init_dataset_table = BigQueryExecuteQueryOperator(
    task_id='init_dataset_table_task',
    gcp_conn_id=connection_id,
    sql = "/sql/init_dataset_table.sql",
    params = {
        'project_id': PROJECT_ID,
        'dataset_id': DATASET_ID,
        'table_name': TABLE_NAME
    }
)

load_data = BigQueryExecuteQueryOperator(
    task_id='load_data_task',
    gcp_conn_id=connection_id,
    sql = "/sql/load_data.sql",
    params = {
        'project_id': PROJECT_ID,
        'dataset_id': DATASET_ID,
        'table_name': TABLE_NAME,
        'bucket_name': BUCKET_NAME,
        'file_path': '/tmp/' + "{{ task_instance.xcom_pull(task_ids='csv_to_gzip_task', key='file_name') }}"
    }
)

end = EmptyOperator(task_id='end', dag=dag)

start >> download_task >> ndjson_to_csv_task >> csv_to_gzip_task >> check_bucket_exist_task
check_bucket_exist_task >> create_bucket >> upload_task
check_bucket_exist_task >> upload_task 
upload_task >> init_dataset_table >> load_data >> end