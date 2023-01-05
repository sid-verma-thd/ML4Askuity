from __future__ import print_function

from datetime import datetime, timedelta
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

default_args = {
    'owner': 'sid',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def simple_sum(*args):
    import logging
    result = 0
    # Iterating over the Python args tuple
    for x in args:
        result += x
    logging.info(f'The sum of values is {result}')
    return result

#get age and push for xcommunication
def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")

#get name and push for xcommunication
def get_name(ti, **kwargs):
    ti.xcom_push(key='first_name', value=kwargs['first_name'])
    ti.xcom_push(key='last_name', value=kwargs['last_name'])

#get age and push for xcommunication
def get_age(ti, **kwargs):
    ti.xcom_push(key='age', value=kwargs['age'])

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    default_args=default_args,
    dag_id='sid_scav_concat_all',
    description='Chaining all the DAGs into single DAG',
    start_date=datetime(2022, 12, 15),
    schedule_interval='@daily'
) as dag:
    start = DummyOperator(task_id="start")
    
    with TaskGroup("Operators", tooltip="concatenating bash and python operators") as concat_operators:
        # An instance of an operator is called a task. In this case, the
        # sum_python adds all list of integers passed
        sum_python = PythonOperator(
            task_id='simple_addition',
            python_callable=simple_sum,
            op_args=[1,2,3,4,5])

        # Likewise, the goodbye_bash task calls a Bash script.
        goodbye_bash = BashOperator(
            task_id='World',
            bash_command='echo Hello World')

        sum_python >> goodbye_bash
    
    with TaskGroup("Xcoms", tooltip="Greeting with concatenating python operators") as concat_xcoms:
        # An instance of an operator is called a task. In this case, the
        # get_name takes in the first and the last name
        
        task1 = PythonOperator(
            task_id='get_name',
            python_callable=get_name,
            op_kwargs={'first_name':'Sid', 'last_name':'Verma'}
        )

        # get_age takes in the age of the person
        task2 = PythonOperator(
            task_id='get_age',
            python_callable=get_age,
            op_kwargs={'age':30}
        )

        # greet finally outputs the greeting message
        task3 = PythonOperator(
        task_id='greet',
        python_callable=greet
        )

        [task1, task2] >> task3


    with TaskGroup("Read_Write", tooltip="R_W using BigQueryOperators") as concat_rw:
        # An instance of an operator is called a task. In this case, the
        # Bigquery insert job reads filtered payload table and writes it to a new table in sid_workspace
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
        # Bigquery to gcs operato reads filtered payload table and writes it to a gcs location

        rw_task_gcs =BigQueryToGCSOperator(
            task_id='rw_bq_gcs_task',
            source_project_dataset_table='analytics-askuity-thd.notification_inventory_skustore.FILTERED_PAYLOAD_DATA_FLAT',
            destination_cloud_storage_uris='gs://us-central1-askuity-insight-10a8fe40-bucket/dags/sid_scav/filtered_payload_data.txt'
        )

        rw_task >> rw_task_gcs
    

    # Define the order in which the tasks complete by using the >> and <<
    start >> concat_operators >> concat_xcoms >> concat_rw