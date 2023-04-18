from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator
)

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
    schedule=timedelta(days=7),
    start_date=datetime(2023, 4, 18),
    catchup=False,
) as dag:

    # TODO: try triggering spark jar that is in private s3 bucket
    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={
            "name": "new_application",
            "sparkSubmitJobDriver": {
                "entryPoint": "s3://<your-bucket>/<path-to>/your-spark-job.jar",
                "entryPointArguments": ["arg1", "arg2"],
                "sparkSubmitParameters": "--conf spark.executorEnv.YOUR_ENV_VAR=your_env_value"
            }
        },
        aws_conn_id="my_aws_connection"
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        aws_conn_id="my_aws_connection",
        application_id="{{ task_instance.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
    )

    create_serverless_app >> delete_app
