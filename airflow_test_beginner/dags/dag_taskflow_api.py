from airflow.decorators import dag, task
from datetime import datetime, timedelta


default_args = {
    "owner": "andyrews",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

@dag(dag_id="taskflow_api_dag",
     default_args=default_args,
     start_date=datetime(2025, 6, 3),
     schedule_interval="@daily"
    )
     
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_n": "Andy",
            "last_n": "Djing"
        }

    @task()
    def get_age():
        return 30

    @task()
    def greet(first_n, last_n, age):
        print(f"Hello {first_n} {last_n}, I am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(first_n=name_dict["first_n"], last_n=name_dict["last_n"], age=age)

greet_dag = hello_world_etl()