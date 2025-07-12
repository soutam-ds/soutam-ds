from airflow.sdk import DAG,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from io import StringIO

@DAG(
    dag_id="user_processing",
    start_date=datetime(2025,7,1),
    schedule="@daily",
    catchup=False,
)

def s3_etl():

    @task
    def read_s3_file():
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')

        files = s3_hook.list_keys(bucket_name='stm-np-all-landing', prefix='airflow_s3/')
        if not files:
            raise ValueError("No files found in the specified S3 bucket and prefix.")
        
        return files
    @task
    def process_file(file_key):
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        file_content = s3_hook.read_key(key=file_key, bucket_name='stm-np-all-landing')
        
        # Assuming the file is a CSV
        df = pd.read_csv(StringIO(file_content))
        # Perform any processing on the DataFrame here
        processed_data = df.head()
        return file_key
    
    list_files = read_s3_file()
    read_s3_file.expand(file_key=list_files)

s3_etl()