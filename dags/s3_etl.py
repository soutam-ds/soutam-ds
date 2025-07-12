from airflow.decorators import dag,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from io import StringIO

@dag(
    dag_id="s3_etl",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
)

def s3_etl():

    @task
    def read_s3_file():
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')

        files = s3_hook.list_keys(bucket_name='stm-np-all-landing', prefix='airflow_s3/')
        print(f"Files found: {files}")
        if not files:
            raise ValueError("No files found in the specified S3 bucket and prefix.")
        
        return files
    @task
    def process_file(file_key):
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        file_content = s3_hook.read_key(key=file_key, bucket_name='stm-np-all-landing')
        
        # Assuming the file is a CSV
        try:
            df = pd.read_csv(StringIO(file_content))
        except pd.errors.EmptyDataError:
            print(f"File {file_key} has no columns to parse. Skipping.")
            return f"{file_key} has no columns"
        # Perform any processing on the DataFrame here
        processed_data = df.head()
        return file_key
    
    list_files = read_s3_file()
    process_file.expand(file_key=list_files) #expand helps in fanout the process to all files
    read_s3_file >> process_file

s3_etl = s3_etl()