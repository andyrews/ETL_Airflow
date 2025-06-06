from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}   

with DAG(
    dag_id="dag_minio_v1",
    default_args=default_args,
    description="A DAG to demonstrate Minio Operator",
    start_date=datetime(2025, 6, 5),
    schedule_interval="@daily",
) as dag:
    task1 = S3KeySensor(
        task_id="sensor_minio",
        bucket_name="airflow",
        bucket_key="data.csv",
        aws_conn_id="minio_conn",
        mode="poke",
        poke_interval=5,
        timeout=30
    )
    task1