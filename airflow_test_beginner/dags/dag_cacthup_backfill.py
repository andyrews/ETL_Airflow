from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="catchup_backfill_dag_v1",
    default_args=default_args,
    description="A DAG to demonstrate catchup and backfill functionality",
    start_date=datetime(2025, 6, 3, 12, 50),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo Catchup and Backfill Example"
    )
