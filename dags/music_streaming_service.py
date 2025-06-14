from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="glue_music_pipeline_dag",
    default_args=default_args,
    description="Orchestrates ETL for metadata, streaming enrichment, and KPI computation",
    schedule_interval=None,  # Set to "@daily" or cron expression if needed
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["glue", "mss", "music"],
) as dag:

    # Job 1: Metadata Processing
    job_metadata = GlueJobOperator(
        task_id="etl_metadata",
        job_name="etl-metadata",
        region_name="us-east-1",
        iam_role_name="arn:aws:iam::648637468459:role/lab1-gluerole",
        wait_for_completion=True,
    )

    # Job 2: Enrichment with streaming data
    job_streaming = GlueJobOperator(
        task_id="streaming_enrichment",
        job_name="music-streaming-pipeline",
        region_name="us-east-1",
        iam_role_name="arn:aws:iam::648637468459:role/lab1-gluerole",
        wait_for_completion=True,
    )

    # Job 3: Compute KPIs and upsert to Redshift
    job_kpi = GlueJobOperator(
        task_id="compute_kpi",
        job_name="compute-kpi",
        region_name="us-east-1",
        iam_role_name="arn:aws:iam::648637468459:role/lab1-gluerole",
        wait_for_completion=True,
    )

    # DAG execution order
    job_metadata >> job_streaming >> job_kpi
