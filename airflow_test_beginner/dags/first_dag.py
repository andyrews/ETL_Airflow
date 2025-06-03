from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    default_args=default_args,
    dag_id="sample_dag_v3",
    description="A simple DAG for demonstration purposes",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 3, 12,50),
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo Hello World!"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo This is the second task!"
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo This is the third task!"
    )

    #task1.set_downstream(task2)
    #task1.set_downstream(task3)
    task1 >> [task2, task3]
