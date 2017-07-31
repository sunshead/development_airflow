from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG('simple_sql_dag', description='Test SQL DAG',
           schedule_interval='0 12 * * *',
           start_date=datetime(2017, 7, 18), catchup=False)

t1 = PostgresOperator(task_id='simple_sql_task', sql='/test_sql/test_query.sql', dag=dag, postgres_conn_id='local_redshift_bridge')

t2 = PostgresOperator(task_id='second_simple_sql_task', sql='/test_sql/test_query.sql', dag=dag, postgres_conn_id='local_redshift_bridge')

t1 >> t2
