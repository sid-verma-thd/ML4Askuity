import datetime
from datetime import datetime, timedelta
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator


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

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    default_args=default_args,
    dag_id='sid_scav_operators',
    description='using python and bash operators',
    start_date=datetime(2022, 12, 15),
    schedule_interval='@daily'
) as dag:
    

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    sum_python = python_operator.PythonOperator(
        task_id='simple_addition',
        python_callable=simple_sum,
        op_args=[1,2,3,4,5])

    # Likewise, the goodbye_bash task calls a Bash script.
    goodbye_bash = bash_operator.BashOperator(
        task_id='World',
        bash_command='echo Hello World')

    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    sum_python >> goodbye_bash