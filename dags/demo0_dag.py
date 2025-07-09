from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 7, 8),
    schedule="@daily", #schedule_interval changed with schedule string in airflow 2.9
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    task2 = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello World!"'
    )

    task1 >> task2
