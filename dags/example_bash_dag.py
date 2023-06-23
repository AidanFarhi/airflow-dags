from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='print_messages',
    start_date=datetime(2023, 6, 22),
    schedule_interval=None
)

# Define the BashOperators
task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, Airflow!"',
    dag=dag
)

task2 = BashOperator(
    task_id='print_world',
    bash_command='echo "Airflow says: World!"',
    dag=dag
)

task3 = BashOperator(
    task_id='print_example',
    bash_command='echo "This is an example."',
    dag=dag
)

# Set the task dependencies
task1 >> task2 >> task3
