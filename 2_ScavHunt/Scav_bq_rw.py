import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
default_args = {
    'owner': 'sid',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='sid_scav_bq_rw',
    description='A simple DAG that reads from a BQ table and outputs the results to another BQ table',
    start_date=datetime(2022, 12, 15),
    schedule_interval='@daily'
) as dag:

    # Set up the task to read data from a BQ table

    rw_task =BigQueryInsertJobOperator(
            task_id='rw_task',
            configuration={
                "query": {
                    "query": 'SELECT * FROM `analytics-askuity-thd.notification_inventory_skustore.FILTERED_PAYLOAD_DATA_FLAT`',
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": 'analytics-askuity-thd',
                        "datasetId": 'sid_workspace',
                        "tableId": 'sid_rw'
                        },
                    'allowLargeResults': True,
                    }
            },
        )