import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
default_args = {
    'owner': 'sid',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='sid_scav_bq_gcs',
    description='A simple DAG that reads from a BQ table and outputs the results to GCS table',
    start_date=datetime(2022, 12, 15),
    schedule_interval='@daily'
) as dag:

    # Set up the task to read data from a BQ table

    rw_task =BigQueryToGCSOperator(
            task_id='rw_bq_gcs_task',
            source_project_dataset_table='analytics-askuity-thd.notification_inventory_skustore.FILTERED_PAYLOAD_DATA_FLAT',
            destination_cloud_storage_uris='gs://us-central1-askuity-insight-10a8fe40-bucket/dags/sid_scav/filtered_payload_data.txt'
        )