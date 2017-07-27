from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def first_run():
    return 'First Run'

dag = DAG('first_dag', description='Test DAG',
           schedule_interval='0 12 * * *',
           start_date=datetime(2017, 7, 18), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

first_run_operator = PythonOperator(task_id='first_run', python_callable=first_run, dag=dag)

dummy_operator >> first_run_operator
