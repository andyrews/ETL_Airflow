from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}


def get_name(ti):
    ti.xcom_push(key="first_name", value="Andy")
    ti.xcom_push(key="last_name", value="Djing")


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello {first_name}! You are from the {last_name} family at age {age}.")


def get_age(ti):
    ti.xcom_push(key="age", value=30)


with DAG(
    default_args=default_args,
    dag_id="python_op_v2",
    description="Python Operator DAG",
    start_date=datetime(2025, 6, 3),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
    )

    task2 = PythonOperator(task_id="get_name", python_callable=get_name)

    task3 = PythonOperator(task_id="get_age", python_callable=get_age)

    [task2, task3] >> task1
