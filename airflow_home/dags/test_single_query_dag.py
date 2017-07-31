from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'motiv',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 30),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('single_query_dag', description='single query dag',
           default_args=default_args,
           schedule_interval='0 12 * * *',
           start_date=datetime(2017, 7, 28), catchup=False)

t1 = PostgresOperator(task_id='test_sql_t10_syslog_aa', sql='/test_sql/t10_syslog_aa.sql', dag=dag, postgres_conn_id='local_redshift_bridge')
