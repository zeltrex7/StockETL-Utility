from datetime import datetime , timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import Scripts.WebScrapStockDataBulk  as extract
import Scripts.consume_data  as load
# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="data_ingestion", start_date=datetime(2024, 7, 7), schedule=timedelta(days=1)) as dag:
    # Tasks are represented as operators
    extract = PythonOperator(task_id="Extract", python_callable=extract.main)
    load = PythonOperator(task_id="Load", python_callable=load.main ,retries=3)

   
    # Set dependencies between tasks
    extract >> load