from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow import hooks
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 13, 11, 39, 0),
    'email': ['prakarsh2512@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
dag = DAG(
    'check_emr_steps',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    # schedule_interval='0 3 * * *'
    schedule_interval=timedelta(minutes=5)
)

def poke(ds, **kwargs):
    hook = hooks.S3_hook.S3Hook(aws_conn_id='aws_s3')
    st=hook.read_key(key='prod_deployment/conf/athena_all_tables', bucket_name='bounce-data-platform')
    loop=st.split("\n")
    for string in loop
        print(string)

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=poke,
    dag=dag,
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

run_this >> t1

