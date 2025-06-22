from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone
from docker.types import Mount
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id='local_glue_etl_pipeline',
    description='Run local Glue jobs with DockerOperator (fail-fast)',
    start_date=timezone.utcnow(),
    schedule='@hourly',
    catchup=False,
    default_args=default_args,
    tags=['local', 'glue', 'spark'],
)

common_args = {
    "image": "public.ecr.aws/glue/aws-glue-libs:5",
    "network_mode": "airflow",
    "mounts": [
        Mount(source=os.path.expanduser("~/.aws"), target="/home/glue_user/.aws", type="bind", read_only=True),
        Mount(source=os.getcwd(), target="/home/glue_user/workspace", type="bind"),
    ],
    "working_dir": "/home/glue_user/workspace/glue_scripts",
    "auto_remove": "force",  # <- fixed
    "mount_tmp_dir": False,
    "do_xcom_push": False,
    "execution_timeout": timedelta(minutes=3),
    "trigger_rule": 'all_success',
}


with dag:
    discover_csv_files = DockerOperator(
        task_id='discover_csv_files',
        command="spark-submit streaming_data_ingestion.py --step discover",
        **common_args,
    )

    with TaskGroup("streaming_ingestion", tooltip="Streaming Ingestion Tasks") as streaming_group:
        ingest_csv_stream = DockerOperator(
            task_id='ingest_csv_stream',
            command="spark-submit streaming_data_ingestion.py --step ingest",
            **common_args,
        )

    with TaskGroup("batch_transformation", tooltip="Batch ETL Steps") as batch_group:
        load_data = DockerOperator(
            task_id='load_data',
            command="spark-submit batch_transformation.py --step load",
            **common_args,
        )

        validate_data = DockerOperator(
            task_id='validate_data',
            command="spark-submit batch_transformation.py --step validate",
            **common_args,
        )

        join_transform_data = DockerOperator(
            task_id='join_transform_data',
            command="spark-submit batch_transformation.py --step join",
            **common_args,
        )

        archive_processed_data = DockerOperator(
            task_id='archive_processed_data',
            command="spark-submit batch_transformation.py --step archive",
            **common_args,
        )

        load_data >> validate_data >> join_transform_data >> archive_processed_data

    discover_csv_files >> streaming_group >> batch_group
