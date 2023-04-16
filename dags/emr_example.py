"""
This is an example dag for a AWS EMR Pipeline with auto steps.
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago

# TODO: Speficy a log s3 URI and set the EmrJobFlowSensor to wait a few minutes before trying to find the log bucket.

SPARK_STEPS = [
    {
        'Name': 'calculate_pi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'PiCalc',
    'ReleaseLabel': 'emr-5.29.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}
# [END howto_operator_emr_automatic_steps_config]

with DAG(
    dag_id='emr_job_flow_automatic_steps_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
    },
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval='0 3 * * *',
    tags=['example'],   
) as dag:

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='my_aws_connection',
        # emr_conn_id='my_aws_connection',
    )

    # Wait for EMR cluster to reach a terminal state
    job_flow_sensor = EmrJobFlowSensor(
        task_id='watch_job_flow',
        aws_conn_id='my_aws_connection',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        timeout=10_000,
    )

    # Final task to mark the DAG as successful
    success = BashOperator(
        task_id='success',
        bash_command="echo 'SUCESS!'"
    )

    job_flow_creator >> job_flow_sensor >> success