from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from datetime import datetime
import requests
import logging

@dag(
    dag_id="user_processing",
    start_date=datetime(2025,7,1),
    schedule="@daily",
    catchup=False,
)
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def api_status():
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/")
        logging.info(f"API status response: {response.status_code}")
        if response.status_code == 200:
            # sensor is done; return data
            return response.json()
        else:
            # returning None -> keep poking
            return None

    # Example: print the data after API is ready
    @task
    def process_user_data(user_data):
        logging.info(f"Fetched user data: {user_data}")

    # Set dependencies
    user_data = api_status()
    process_user_data(user_data)  # after sensor returns, process it
    create_table >> process_user_data(user_data)

user_processing()
