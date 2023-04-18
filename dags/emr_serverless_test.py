from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator
)

# Define the DAG and its tasks.
with DAG(
    'emr_serverless_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 1, 1),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Run Spark job on EMR Serverless',
    schedule_interval='0 0 * * *',  # Run daily at midnight UTC
) as dag:

    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={
            "name": "new_application"
        },
        aws_conn_id="my_aws_connection"
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_application",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
    )

    create_serverless_app >> delete_app
