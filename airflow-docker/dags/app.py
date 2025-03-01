from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Define default_args (optional)
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_first_dag',  # DAG ID
    default_args=default_args,
    description='My first DAG in Airflow',
    schedule_interval=timedelta(days=1),  # How often the DAG runs
    start_date=datetime(2023, 1, 1),
    catchup=False,  # If False, DAG won’t backtrack to previous dates
)

# Define tasks
start_task = DummyOperator(
    task_id='start_task', 
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

# Define task dependencies (order of execution)
start_task >> end_task
