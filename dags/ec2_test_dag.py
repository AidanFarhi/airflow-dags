from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_ec2_instance():
    aws_access_key = Variable.get('aws_access_key')
    aws_secret_key = Variable.get('aws_secret_key')
    aws_region = Variable.get('aws_region')
    github_username = Variable.get('github_username')
    github_repo = Variable.get('github_repo')
    github_path = Variable.get('github_path')
    ami_id = Variable.get('ami_id')
    key_pair_name = Variable.get('key_pair_name')
    AWS_CONFIG = {
        'aws_access_key_id': aws_access_key,
        'aws_secret_access_key': aws_secret_key,
        'region_name': aws_region
    }
    GITHUB_CONFIG = {
        'owner': github_username,
        'repo': github_repo,
        'path': github_path,
        'raw': 'https://raw.githubusercontent.com'
    }
    ec2 = boto3.resource('ec2', **AWS_CONFIG)
    # Fetch the UserData script from GitHub
    url = f"{GITHUB_CONFIG['raw']}/{GITHUB_CONFIG['owner']}/{GITHUB_CONFIG['repo']}/main/{GITHUB_CONFIG['path']}"
    response = requests.get(url)
    response.raise_for_status()
    userdata = response.text
    # Launch an EC2 instance
    instance = ec2.create_instances(
        ImageId=ami_id,
        MinCount=1,
        MaxCount=1,
        KeyName=key_pair_name,
        InstanceType='t2.micro',
        UserData=userdata,
    )[0]
    print(f"EC2 instance {instance.id} launched.")

with DAG(
    'ec2_startup_script',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start_ec2 = PythonOperator(
        task_id='start_ec2',
        python_callable=start_ec2_instance
    )

    start_ec2
