import json
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

with DAG(
    "location_iq_data_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0
    },
    description="Data pipeline for LocationIQ project",
    schedule=timedelta(days=7),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["location-iq"],
) as dag:
    extract_col = LambdaInvokeFunctionOperator(
        task_id="extract-cost-of-living",
        function_name="extract-cost-of-living-data",
        aws_conn_id="my_aws_connection",
        retries=3
    )

    load_col = LambdaInvokeFunctionOperator(
        task_id="load-cost-of-living",
        function_name="load-cost-of-living-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),  # TODO make this overridable using Airflow vars
    )

    extract_crime = LambdaInvokeFunctionOperator(
        task_id="extract-crime",
        function_name="extract-crime-data",
        aws_conn_id="my_aws_connection",
        retries=3
    )

    load_crime = LambdaInvokeFunctionOperator(
        task_id="load-crime",
        function_name="load-crime-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),  # TODO make this overridable using Airflow vars
    )

    extract_listing = LambdaInvokeFunctionOperator(
        task_id="extract-listing",
        function_name="extract-listing-data",
        aws_conn_id="my_aws_connection",
        retries=3
    )

    load_listing = LambdaInvokeFunctionOperator(
        task_id="load-listing",
        function_name="load-listing-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),  # TODO make this overridable using Airflow vars
    )

    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={
            "name": "etl_application",
            "networkConfiguration": {
                "subnetIds": [
                    Variable.get("VPC_PRIVATE_SUBNET"),
                ],
                "securityGroupIds": [
                    Variable.get("VPC_EMR_SG"),
                ],
            },
        },
        aws_conn_id="my_aws_connection",
    )

    run_spark_job = EmrServerlessStartJobOperator(
        task_id="run_location_summary_etl_job",
        aws_conn_id="my_aws_connection",
        application_id="{{ ti.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
        execution_role_arn=Variable.get("EMR_EXECUTION_ROLE_ARN"),
        job_driver={
            "sparkSubmit": {
                "entryPoint": Variable.get("SPARK_JOB_S3_PATH"),
                "entryPointArguments": [
                    Variable.get("SNOWFLAKE_URL"),
                    Variable.get("SNOWFLAKE_USER"),
                    Variable.get("SNOWFLAKE_PASSWORD"),
                    Variable.get("SNOWFLAKE_DATABASE"),
                    Variable.get("SNOWFLAKE_SCHEMA"),
                    Variable.get("SNOWFLAKE_WAREHOUSE"),
                ],
                "sparkSubmitParameters": "--class App",
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": Variable.get("S3_LOG_BUCKET_URI")}}
        },
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_emr_serverless_app",
        aws_conn_id="my_aws_connection",
        application_id="{{ ti.xcom_pull(task_ids='create_emr_serverless_app', key='return_value') }}",
    )

    extract_col >> load_col
    extract_crime >> load_crime
    extract_listing >> load_listing
    [load_col, load_crime, load_listing] >> create_serverless_app >> run_spark_job >> delete_app
