import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook  # <-- added

# === Config ===
RAW_BUCKET = "lab3-raw"
CURATED_BUCKET = "lab3-curated"
PRESENTATION_BUCKET = "lab3-presentation-data"
PROCESSED_STREAM_PREFIX = "processed-streams/"
TRANSFORMED_DATA_PREFIX = "transformed-data/latest/"

STREAMING_JOB = "streaming_data_ingestion"
KPI_JOB = "kpi_transformation"

def get_s3_client():
    import boto3
    conn = BaseHook.get_connection("aws_default")
    session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="us-east-1"
    )
    return session.client("s3")

def check_batch_transform_completion():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix=TRANSFORMED_DATA_PREFIX)
        if 'Contents' in response and len(response['Contents']) > 0:
            print("✅ batch_transform completed - found transformed data")
            return True
        else:
            print("❌ batch_transform not completed - no transformed data found")
            return False
    except Exception as e:
        print(f"Error checking batch_transform completion: {e}")
        return False

def cleanup_processed_streams():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=PROCESSED_STREAM_PREFIX)
        remaining_files = len(response.get('Contents', []))
        print(f"Remaining processed stream files: {remaining_files}")
        if remaining_files > 0:
            print("Note: Some processed stream files still exist (may be processing)")
    except Exception as e:
        print(f"Error during cleanup check: {e}")

def log_pipeline_completion():
    import json
    from datetime import datetime

    s3 = get_s3_client()
    completion_log = {
        "pipeline_run_id": f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        "completion_time": datetime.utcnow().isoformat(),
        "status": "SUCCESS",
        "components_completed": [
            "streaming_data_ingestion",
            "lambda_trigger",
            "batch_transform", 
            "kpi_transformation"
        ]
    }

    s3.put_object(
        Bucket=PRESENTATION_BUCKET,
        Key=f"logs/pipeline_completion_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
        Body=json.dumps(completion_log, indent=2).encode()
    )
    print("✅ Pipeline completion logged")

# === DAG Definition ===
with DAG(
    dag_id="music_streaming_pipeline",
    start_date=pendulum.now("UTC").subtract(days=1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["music-streaming", "etl", "glue", "on-demand"],
    description="On-demand music streaming data pipeline with event-driven batch processing"
) as dag:

    start = EmptyOperator(task_id="pipeline_start")

    streaming_ingestion = GlueJobOperator(
        task_id="run_streaming_data_ingestion",
        job_name=STREAMING_JOB,
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    with TaskGroup("wait_for_batch_processing", tooltip="Wait for Lambda-triggered batch processing") as batch_wait_group:
        wait_for_transformed_data = S3KeySensor(
            task_id="wait_for_transformed_data",
            bucket_name=CURATED_BUCKET,
            bucket_key=TRANSFORMED_DATA_PREFIX + "*",
            wildcard_match=True,
            poke_interval=60,
            timeout=60 * 45,
            mode="poke",
            soft_fail=False,
        )

        verify_batch_completion = PythonOperator(
            task_id="verify_batch_transform_completion",
            python_callable=check_batch_transform_completion,
        )

        wait_for_transformed_data >> verify_batch_completion

    with TaskGroup("kpi_processing", tooltip="KPI transformation and metrics") as kpi_group:
        kpi_transformation = GlueJobOperator(
            task_id="run_kpi_transformation",
            job_name=KPI_JOB,
            aws_conn_id="aws_default", 
            region_name="us-east-1",
            wait_for_completion=True,
        )

        cleanup_check = PythonOperator(
            task_id="cleanup_processed_streams",
            python_callable=cleanup_processed_streams,
        )

        kpi_transformation >> cleanup_check

    log_completion = PythonOperator(
        task_id="log_pipeline_completion",
        python_callable=log_pipeline_completion,
    )

    end = EmptyOperator(task_id="pipeline_end")

    # Final task flow
    start >> streaming_ingestion >> batch_wait_group >> kpi_group >> log_completion >> end
