import sklearn
import matplotlib

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

def get_sklearn():
    print(f"scikit-learn with version: {sklearn.__version__}")

def get_matplotlib():
    print(f"matplotlib with version: {matplotlib.__version__}")

with DAG(
    default_args=default_args,
    dag_id="dag_python_deps_v1",
    start_date=datetime(2025, 6, 5),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="sklearn_task",
        python_callable=get_sklearn,
    )

    task2 = PythonOperator(
        task_id="matplotlib_task",
        python_callable=get_matplotlib,
    )
    task1 >> task2

