from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator 

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_postgres",
    start_date=datetime(2025, 6, 4),
    schedule_interval="@daily",  # Runs once a day
) as dag:
    task1 = PostgresOperator(
        task_id="task1",
        postgres_conn_id="postgres_localhost",  # Ensure this connection is set up in Airflow
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id CHARACTER varying,
                primary key (dt, dag_id)
            );
        """,
    )

    task2 = PostgresOperator(
        task_id="task2",
        postgres_conn_id="postgres_localhost",
        sql="""
            INSERT INTO dag_runs (dt, dag_id)
            VALUES ('{{ ds }}', '{{ dag.dag_id }}');
        """,
    )

    task3 = PostgresOperator(
        task_id="delete_task",
        postgres_conn_id="postgres_localhost",
        sql="""
            DELETE FROM dag_runs
            WHERE DT = '{{ ds }}' AND dag_id = '{{ dag.dag_id }}';
        """,
    )
    task1 >> task3 >>  task2