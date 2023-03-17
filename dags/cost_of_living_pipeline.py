import json
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


with DAG(
    "cost_of_living_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Cost of living data pipeline for LocationIQ project",
    schedule=timedelta(days=7),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["location-iq"],
) as dag:

    t1 = LambdaInvokeFunctionOperator(
        task_id='extract-data-from-webpage',
        function_name='extract-cost-of-living-data',
        invocation_type='Event',
        aws_conn_id='my_aws_connection'
    )

    t2 = LambdaInvokeFunctionOperator(
        task_id='load-data-to-snowflake',
        function_name='load-cost-of-living-data-to-snowflake',
        aws_conn_id='my_aws_connection',
        payload=json.dumps({'extractDate': str(date.today())}) #TODO make this overridable using Airflow vars
    )

    t1 >> t2