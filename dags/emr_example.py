"""
This is an example dag for a AWS EMR Pipeline with auto steps.
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateJobRunOperator, 
    EmrServerlessWaitForJobRunOperator
)

from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago


SPARK_STEPS = [    
    {        
        'Name': 'SparkPi',        
        'ActionOnFailure': 'CONTINUE',        
        'HadoopJarStep': {            
            'Jar': 'command-runner.jar',            
            'Args': [
                'spark-submit', 
                '--deploy-mode', 'cluster', 
                '--class', 'org.apache.spark.examples.SparkPi', 
                '--master', 'yarn', 
                '--executor-memory', '1g', 
                '--num-executors', '2', 
                '/usr/lib/spark/examples/jars/spark-examples.jar', 
                '100'
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'PiCalc',
    'ReleaseLabel': 'emr-6.10.0',
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
    'LogUri': 's3://afarhidev-log-bucket/emr-logs/'
}

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
    schedule_interval='0 13 * * *',
    tags=['example'],   
) as dag:

    job_flow_creator = EmrServerlessCreateJobRunOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='my_aws_connection'
    )

    # Wait for EMR cluster to reach a terminal state
    job_flow_sensor = EmrJobFlowSensor(
        task_id='watch_job_flow',
        aws_conn_id='my_aws_connection',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        timeout=1_800,
    )

    # Final task to mark the DAG as successful
    success = BashOperator(
        task_id='success',
        bash_command="echo 'SUCESS!'"
    )

    job_flow_creator >> job_flow_sensor >> success