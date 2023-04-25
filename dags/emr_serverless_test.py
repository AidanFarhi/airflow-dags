from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor


with DAG(
    'emr_serverless_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    description='Run Spark job on EMR Serverless',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 4, 18),
    catchup=False,
) as dag:

    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={
            "name": "etl_application"
        },
        aws_conn_id="my_aws_connection"
    )

    run_spark_job = EmrServerlessStartJobOperator(
        task_id="run_location_summary_etl_job",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
        # execution_role_arn=Variable.get("EMR_EXECUTION_ROLE_ARN"),
        job_driver={
            "sparkSubmit": {
                "entryPoint": Variable.get("SPARK_JOB_S3_PATH"),
            }
        },
        configuration_overrides={
            "environment": {
                "SNOWFLAKE_DATABASE": Variable.get("SNOWFLAKE_DATABASE"),
                "SNOWFLAKE_PASSWORD": Variable.get("SNOWFLAKE_PASSWORD"),
                "SNOWFLAKE_SCHEMA": Variable.get("SNOWFLAKE_SCHEMA"),
                "SNOWFLAKE_URL": Variable.get("SNOWFLAKE_URL"),
                "SNOWFLAKE_USER": Variable.get("SNOWFLAKE_USER"),
                "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE")
            },
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": Variable.get("S3_LOG_BUCKET_URI")}
            }
        }
    )

    wait_for_job = EmrServerlessJobSensor(
        task_id="wait_for_job",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
        job_run_id="{{ task_instance.xcom_pull(task_ids='run_location_summary_etl_job', key='return_value') }}",
    )   

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_emr_serverless_app",
        aws_conn_id="my_aws_connection",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
    )

    create_serverless_app >> run_spark_job >> wait_for_job >> delete_app
