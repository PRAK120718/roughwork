from datetime import timedelta,datetime
import airflow
from airflow import DAG
from airflow import hooks, settings
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 17, 11, 20, 0, tzinfo=local_tz),
    'email': ['prakarsh2512@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 0,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'check_emr_steps',
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    # schedule_interval='0 3 * * *'
    schedule_interval=timedelta(minutes=5)
)
step_adder=[]
step_checker=[]
def poke():
    hook = hooks.S3_hook.S3Hook(aws_conn_id='aws_s3')
    job_flow_id = "j-2ASQREUMPJ0Y7"
    aws_conn_id = 'aws_emr'
    st=hook.read_key(key='prod_deployment/conf/athena_all_tables', bucket_name='bounce-data-platform')
    loop=st.split("\n")
    for i in range(len(loop)):
        steps=[
            {
                'Name': 'test step',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hive',
                        '-e',
                        loop[i]
                    ]
                }
            }
        ]
        step_addr = EmrAddStepsOperator(
            task_id='add_steps '+str(i),
            job_flow_id="j-2ASQREUMPJ0Y7",
            aws_conn_id='aws_emr',
            steps=steps,
            dag=dag
        )
        step_adder.append(step_addr)
        step_checkr = EmrStepSensor(
            task_id='watch_step'+str(i),
            job_flow_id="j-2ASQREUMPJ0Y7",
            step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
            aws_conn_id='aws_emr',
            dag=dag
        )
        step_checker.append(step_checkr)

def execute():
    chain(step_adder,step_checker)

def main():
    poke()
    execute()

if __name__=="__ main__":
    main()


