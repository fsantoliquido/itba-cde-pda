from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from module_etl.etl import etl_run


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='DAG PARA EL ETL A RS',
    schedule_interval='@daily', 
)

run_etl_task = PythonOperator(
    task_id='run_youtube_etl',
    python_callable=etl_run, 
    dag=dag,
)

run_etl_task