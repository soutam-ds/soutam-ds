from airflow.datasets import Dataset
from airflow.decorators import dag, task
from datetime import datetime

landing = Dataset("s3://stm-np-all-landing/airflow_s3/")

@dag(start_date=datetime(2025,7,1), schedule_interval=None, catchup=False)
def dataset_upstream():
    @task(outlets={"landing": landing})
    def write_to_s3():
        import pandas as pd
        from io import StringIO
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # Create a sample DataFrame
        data = {
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        }
        df = pd.DataFrame(data)

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Write to S3
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='sample_data.csv',
            bucket_name='stm-np-all-landing',
            replace=True
        )
    write_to_s3()
dataset_upstream()