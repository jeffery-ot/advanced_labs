# music_streaming_pipeline.py

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.utils.task_group import TaskGroup

# === Config ===
RAW_BUCKET = "lab3-raw"
STREAM_PREFIX = "processed-streams/"
GLUE_BATCH_JOB = "batch_transformation"
GLUE_KPI_JOB = "kpi_transformation"

def archive_stream_data():
    import boto3
    s3 = boto3.client("s3")
    src_prefix = STREAM_PREFIX
    dst_prefix = f"archives/{STREAM_PREFIX}"
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=src_prefix)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        new_key = key.replace(src_prefix, dst_prefix, 1)
        s3.copy_object(Bucket=RAW_BUCKET,
                       CopySource={"Bucket": RAW_BUCKET, "Key": key},
                       Key=new_key)
        s3.delete_object(Bucket=RAW_BUCKET, Key=key)

def log_kpi_metrics():
    import boto3, logging, pendulum
    s3 = boto3.client("s3")
    content = "timestamp,total,valid,dropped\n"
    content += f"{pendulum.now('UTC')},{1000},{950},{50}\n"
    s3.put_object(
        Bucket="lab3-presentation-data",
        Key="logs/validation_metrics.csv",
        Body=content.encode()
    )
    logging.info("✅ KPI metrics logged to S3")

with DAG(
    dag_id="music_streaming_pipeline",
    start_date=pendulum.now("UTC").subtract(days=1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["lab3", "aws", "glue"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_stream = S3KeySensor(
        task_id="wait_for_processed_streams",
        bucket_name=RAW_BUCKET,
        bucket_key=STREAM_PREFIX + "*",
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 30,
        mode="poke",
    )

    with TaskGroup("batch_processing", tooltip="Batch load & transform") as batch_group:
        batch_load = GlueJobOperator(
            task_id="batch_load_users_songs",
            job_name=GLUE_BATCH_JOB,
            script_args={"--step": "load"},
            aws_conn_id="aws_default",
            region_name="us-east-1",
            wait_for_completion=True,
        )

        batch_transform = GlueJobOperator(
            task_id="batch_transform_and_join",
            job_name=GLUE_BATCH_JOB,
            script_args={"--step": "transform"},
            aws_conn_id="aws_default",
            region_name="us-east-1",
            wait_for_completion=True,
        )

        archive_raw = PythonOperator(
            task_id="archive_processed_streams",
            python_callable=archive_stream_data,
        )

        batch_load >> batch_transform >> archive_raw

    with TaskGroup("kpi_processing", tooltip="KPI transform & log") as kpi_group:
        kpi_transform = GlueJobOperator(
            task_id="run_kpi_transformation",
            job_name=GLUE_KPI_JOB,
            aws_conn_id="aws_default",
            region_name="us-east-1",
            wait_for_completion=True,
        )

        log_metrics = PythonOperator(
            task_id="log_kpi_metrics",
            python_callable=log_kpi_metrics,
        )

        kpi_transform >> log_metrics

    end = EmptyOperator(task_id="end")

    start >> wait_for_stream >> batch_group >> kpi_group >> end
