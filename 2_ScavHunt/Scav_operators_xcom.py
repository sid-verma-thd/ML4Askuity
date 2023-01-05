from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'sid',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")


def get_name(ti, **kwargs):
    ti.xcom_push(key='first_name', value=kwargs['first_name'])
    ti.xcom_push(key='last_name', value=kwargs['last_name'])


def get_age(ti, **kwargs):
    ti.xcom_push(key='age', value=kwargs['age'])


with DAG(
    default_args=default_args,
    dag_id='sid_scav_xcom',
    description='xcom dags using python operator',
    start_date=datetime(2022, 12, 15),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        op_kwargs={'first_name':'Sid', 'last_name':'Verma'}
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
        op_kwargs={'age':30}
    )

    [task2, task3] >> task1