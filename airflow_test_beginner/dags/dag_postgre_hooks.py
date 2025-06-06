import csv
import logging

from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

def postgres_to_s3(ds_nodash, next_ds_nodash):
    #query data from test database
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where orderdate >= %s and orderdate <= %s",
                   (ds_nodash, next_ds_nodash)
                   )
    with NamedTemporaryFile(mode="w", suffix=f"{ds_nodash}") as f:
    #with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()

        cursor.close()
        conn.close()
        logging.info("Saved data to text file in text file")

    #upload data to s3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/{ds_nodash}.txt",
            bucket_name="airflow",
            replace=True,
        )
        logging.info("Saved text file to MinIO S3")

with DAG(
    dag_id="dag_postgres_hooks_v1",
    default_args=default_args,
    description="A DAG to demonstrate default arguments",
    start_date=datetime(2025, 5, 29),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=postgres_to_s3,

    )
    task1