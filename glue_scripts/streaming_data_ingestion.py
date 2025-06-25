import sys
import random
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import uuid

# Set your S3 bucket paths here
source_bucket = "s3://lab3-raw/streams/"
output_bucket = "s3://lab3-raw/processed-streams/"

# Logging config
s3 = boto3.client('s3')
LOG_BUCKET_NAME = 'lab4-raw'
LOG_PREFIX = 'logs/'

def log_to_s3(message: str, level: str = "INFO", context: str = "glue-job"):
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"{LOG_PREFIX}{now.strftime('%Y/%m/%d')}/{context}_{level}_{timestamp}_{unique_id}.log"
    log_content = f"[{timestamp}] [{level}] [{context}] {message}"
    s3.put_object(Bucket=LOG_BUCKET_NAME, Key=filename, Body=log_content.encode("utf-8"))
    print(f"Logged to S3: s3://{LOG_BUCKET_NAME}/{filename}")


def init_glue_job():
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init("hardcoded-ingestion-job", {})
    log_to_s3("Glue job initialized.", context="init")
    return glue_context, job


def discover_csv_files_in_s3(glue_context, s3_uri: str) -> list:
    spark = glue_context.spark_session
    try:
        df = spark.read.csv(s3_uri + "*.csv", header=True, inferSchema=True)
        files = df.inputFiles()
        log_to_s3(f"Discovered {len(files)} CSV files in {s3_uri}.", context="discovery")
        return files
    except Exception as e:
        log_to_s3(f"Failed to discover CSV files: {e}", level="ERROR", context="discovery")
        raise FileNotFoundError(f"No CSV files found at {s3_uri} or unable to list: {e}")


def select_random_files(files: list) -> list:
    k = random.randint(1, len(files))
    selected = random.sample(files, k)
    log_to_s3(f"Randomly selected {k} file(s).", context="selection")
    return selected


def read_csv_as_dynamic_frame(glue_context, file_path: str):
    log_to_s3(f"Reading file {file_path}", context="read")
    return glue_context.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [file_path]},
        transformation_ctx="read_stream_csv"
    )


def write_to_s3(glue_context, dynamic_frame, target_uri: str):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"{target_uri.rstrip('/')}/batch_{timestamp}/"

    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )

    log_to_s3(f"Data written to {output_path}", context="write")


def main():
    glue_context, job = init_glue_job()

    try:
        available_files = discover_csv_files_in_s3(glue_context, source_bucket)

        if not available_files:
            raise FileNotFoundError(f"No CSV files found at {source_bucket}")

        selected_files = select_random_files(available_files)
        log_to_s3(f"Selected files for ingestion: {selected_files}", context="selection")

        for file_path in selected_files:
            df = read_csv_as_dynamic_frame(glue_context, file_path)

            # Show a preview of the DynamicFrame as Spark DataFrame
            preview = df.toDF().limit(10).collect()
            log_to_s3(f"Preview of file {file_path}:\n{preview}", context="preview")

            write_to_s3(glue_context, df, output_bucket)

        log_to_s3("Glue job completed successfully.", context="job")

    except Exception as e:
        log_to_s3(f"Glue job failed: {str(e)}", level="ERROR", context="job")
        job.commit()
        sys.exit(1)

    job.commit()


if __name__ == "__main__":
    main()
