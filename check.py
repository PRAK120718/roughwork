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
    'start_date': datetime(2020, 1, 17, 11, 20, 0,tzinfo=local_tz),
    'email': ['prakarsh2512@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 0,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
ds= "Prakarsh"

HIVE_TEST_STEPS = [
    {
        'Name': 'test step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive',
                '-e',
                'msck repair table dataplatform.booking'
            ]
        }
    }
]


dag = DAG(
    'prakarsh_hive',
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    #schedule_interval='0 3 * * *'
    schedule_interval=timedelta(minutes=5)
)



#
# cluster_creator = EmrCreateJobFlowOperator(
#     task_id='create_job_flow',
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id='aws_default',
#     emr_conn_id='emr_default',
#     dag=dag
# )

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="j-2ASQREUMPJ0Y7",
    aws_conn_id='aws_emr',
    steps=HIVE_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="j-2ASQREUMPJ0Y7",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_emr',
    dag=dag
)

# cluster_remover = EmrTerminateJobFlowOperator(
#     task_id='remove_cluster',
#     job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
#     aws_conn_id='aws_default',
#     dag=dag
# )

#cluster_creator.set_downstream(step_adder)
#step_adder.set_downstream(step_checker)
#step_checker.set_downstream(cluster_remover)
step_adder.set_downstream(step_checker)

