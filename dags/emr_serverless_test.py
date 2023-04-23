from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator
)

# Define the S3 path to the Spark job JAR
spark_job_s3_path = 's3://afarhidev-private-jars/artifacts/location-iq/location-summary-etl-LATEST.jar'

# Define the DAG
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

    # Create the EMR Serverless application
    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={
            "name": "new_application"
        },
        aws_conn_id="my_aws_connection"
    )

    # Start Spark job
    run_spark_job = EmrServerlessStartJobOperator(
        task_id="run_location_summary_etl_job",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": spark_job_s3_path,
            }
        },
    )

    # Delete the EMR Serverless application
    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_emr_serverless_app",
        aws_conn_id="my_aws_connection",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
    )

    # Define the DAG dependencies
    create_serverless_app >> run_spark_job  >> delete_app
