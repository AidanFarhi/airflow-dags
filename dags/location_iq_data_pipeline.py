import json
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.cloud_formation import CloudFormationCreateStackOperator, CloudFormationDeleteStackOperator
from airflow.providers.amazon.aws.sensors.cloud_formation import CloudFormationCreateStackSensor, CloudFormationDeleteStackSensor
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
        "retries": 0,
    },
    description="Data pipeline for LocationIQ project",
    schedule=timedelta(days=60),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["location-iq"],
) as dag:
    
    create_stack = CloudFormationCreateStackOperator(
        task_id='create_stack',
        stack_name=Variable.get('CLOUDFORMATION_STACK_NAME'),
        cloudformation_parameters={
            "StackName": Variable.get('CLOUDFORMATION_STACK_NAME'),
            "TemplateURL": Variable.get('EMR_CLOUDFORMATION_TEMPLATE_URL')
        },
        aws_conn_id='my_aws_connection'
    )

    wait_for_stack_create = CloudFormationCreateStackSensor(
        task_id='wait_for_stack_create',
        stack_name=Variable.get('CLOUDFORMATION_STACK_NAME'),
        aws_conn_id='my_aws_connection'
    )

    extract_col = LambdaInvokeFunctionOperator(
        task_id="ingest-cost-of-living",
        function_name="extract-cost-of-living-data",
        aws_conn_id="my_aws_connection",
        retries=3,
    )

    load_col = LambdaInvokeFunctionOperator(
        task_id="cost-of-living-etl",
        function_name="load-cost-of-living-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),
    )

    extract_crime = LambdaInvokeFunctionOperator(
        task_id="ingest-crime", 
        function_name="extract-crime-data", 
        aws_conn_id="my_aws_connection", 
        retries=3
    )

    load_crime = LambdaInvokeFunctionOperator(
        task_id="crime-etl",
        function_name="load-crime-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),
    )

    extract_listing = LambdaInvokeFunctionOperator(
        task_id="ingest-listing", 
        function_name="extract-listing-data", 
        aws_conn_id="my_aws_connection", 
        retries=3,
        execution_timeout=timedelta(minutes=10)
    )

    load_listing = LambdaInvokeFunctionOperator(
        task_id="listing-etl",
        function_name="load-listing-data-to-snowflake",
        aws_conn_id="my_aws_connection",
        retries=3,
        payload=json.dumps({"extractDate": str(date.today())}),
    )

    create_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create-emr-serverless-app",
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
        task_id="run-location-summary-etl-job",
        aws_conn_id="my_aws_connection",
        application_id="{{ ti.xcom_pull(task_ids='create-emr-serverless-app', key='return_value') }}",
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
        task_id="delete-emr-serverless-app",
        aws_conn_id="my_aws_connection",
        application_id="{{ ti.xcom_pull(task_ids='create-emr-serverless-app', key='return_value') }}",
    )

    delete_stack = CloudFormationDeleteStackOperator(
        task_id='delete_stack',
        stack_name=Variable.get('CLOUDFORMATION_STACK_NAME'),
        aws_conn_id='my_aws_connection'
    )

    wait_for_stack_delete = CloudFormationDeleteStackSensor(
        task_id="wait_for_stack_delete",
        stack_name=Variable.get('CLOUDFORMATION_STACK_NAME'),
        aws_conn_id='my_aws_connection'
    )

    create_stack >> wait_for_stack_create
    extract_col >> load_col
    extract_crime >> load_crime
    extract_listing >> load_listing
    [
        load_col, 
        load_crime, 
        load_listing, 
        create_serverless_app, 
        wait_for_stack_create
    ] >> run_spark_job >> delete_app >> delete_stack >> wait_for_stack_delete
