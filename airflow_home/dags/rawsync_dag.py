from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG('rawsync_dag', description='rawsync dag',
           schedule_interval='0 12 * * *',
           start_date=datetime(2017, 7, 18), catchup=False)

# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t10_syslog', sql='rawsync-t10_syslog.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t7_activity_sleep', sql='rawsync-t7_activity_sleep.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t7_activity_sleep_v2', sql='rawsync-t7_activity_sleep_v2.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t22_steps_activity', sql='rawsync-t22_steps_activity.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t28_storage_counters', sql='rawsync-t28_storage_counters.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t31_finger', sql='rawsync-t31_finger.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t33_battery_time_report', sql='rawsync-t33_battery_time_report.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t34_bpm', sql='rawsync-t34_bpm.sql', dag=dag, postgres_conn_id=)
# rawsync-t10_syslog_operator = PostgresOperator(task_id='rawsync-t38_stuck_accel', sql='rawsync-t38_stuck_accel.sql', dag=dag, postgres_conn_id=)
