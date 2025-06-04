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
    dag_id="dag_with_cron_expression",
    start_date=datetime(2025, 6, 3),
    schedule_interval="* 3 * * Tue-Fri", 
    #At every minute past hour 3 on every day-of-week from Tuesday through Friday.
    #https://crontab.guru/
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo This is a simple DAG with catchup and backfill"
    )
    task1