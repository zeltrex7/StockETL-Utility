from datetime import datetime , timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from WebScrapStockDataBulk import main

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="data_ingestion", start_date=datetime(2024, 7, 6), schedule=timedelta(minutes=30)) as dag:
    # Tasks are represented as operators
    hello = PythonOperator(task_id="Extract", python_callable=main)

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow()