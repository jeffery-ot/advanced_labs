from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def list_s3_files():
    hook = S3Hook(aws_conn_id="aws_default")
    files = hook.list_keys(bucket_name="lab3-curated", prefix="transformed-data/")
    print(f"✅ Found {len(files) if files else 0} files in transformed-data/")

test_s3_access = PythonOperator(
    task_id="test_s3_access",
    python_callable=list_s3_files,
)

start >> test_s3_access >> streaming_ingestion
