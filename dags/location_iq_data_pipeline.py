import json
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


with DAG(
    "location_iq_data_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Data pipeline for LocationIQ project",
    schedule=timedelta(days=7),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["location-iq"],
) as dag:

    extract_col = LambdaInvokeFunctionOperator(
        task_id='extract-cost-of-living',
        function_name='extract-cost-of-living-data',
        invocation_type='Event',
        aws_conn_id='my_aws_connection'
    )

    load_col = LambdaInvokeFunctionOperator(
        task_id='load-cost-of-living',
        function_name='load-cost-of-living-data-to-snowflake',
        aws_conn_id='my_aws_connection',
        payload=json.dumps({'extractDate': str(date.today())}) #TODO make this overridable using Airflow vars
    )

    extract_crime = LambdaInvokeFunctionOperator(
        task_id='extract-crime',
        function_name='extract-crime-data',
        invocation_type='Event',
        aws_conn_id='my_aws_connection'
    )

    load_crime = LambdaInvokeFunctionOperator(
        task_id='load-crime',
        function_name='load-crime-data-to-snowflake',
        aws_conn_id='my_aws_connection',
        payload=json.dumps({'extractDate': str(date.today())}) #TODO make this overridable using Airflow vars
    )

    extract_listing = LambdaInvokeFunctionOperator(
        task_id='extract-listing',
        function_name='extract-listing-data',
        invocation_type='Event',
        aws_conn_id='my_aws_connection'
    )

    load_listing = LambdaInvokeFunctionOperator(
        task_id='load-listing',
        function_name='load-listing-data-to-snowflake',
        aws_conn_id='my_aws_connection',
        payload=json.dumps({'extractDate': str(date.today())}) #TODO make this overridable using Airflow vars
    )

    extract_col >> load_col
    extract_crime >> load_crime
    extract_listing >> load_listing
    