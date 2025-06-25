import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StringType, LongType, TimestampType, DateType
import boto3
from helper_function import log_to_s3

def init_glue_job(job_name: str = "normalized-genre-data-job"):
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(job_name, {})
    log_to_s3("Initialized Glue job", context="main")
    return glue_context, job

def load_and_validate_data(glue_context):
    spark = glue_context.spark_session
    log_to_s3("Reading raw data from s3://lab3-curated/transformed-data/latest/", context="load")
    df_raw = spark.read.parquet("s3://lab3-curated/transformed-data/latest/")

    df_casted = df_raw.selectExpr(
        "CAST(user_id AS STRING) AS user_id",
        "CAST(track_name AS STRING) AS track_name",
        "CAST(track_genre AS STRING) AS track_genre",
        "CAST(duration_ms AS BIGINT) AS duration_ms",
        "CAST(listen_time AS TIMESTAMP) AS listen_time"
    ).withColumn("listen_date", to_date("listen_time"))

    df_casted.createOrReplaceTempView("raw_listens")

    validated_df = spark.sql("""
        SELECT *
        FROM raw_listens
        WHERE 
            user_id IS NOT NULL AND
            track_name IS NOT NULL AND
            track_genre IS NOT NULL AND
            duration_ms > 0 AND
            listen_time IS NOT NULL AND
            listen_date IS NOT NULL
    """)

    total_rows = df_casted.count()
    valid_rows = validated_df.count()
    dropped_rows = total_rows - valid_rows

    log_to_s3(f"Validation completed. Total: {total_rows}, Valid: {valid_rows}, Dropped: {dropped_rows}", context="validate")
    return validated_df, total_rows, valid_rows, dropped_rows

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)
        log_to_s3(f"Created bucket: {bucket_name}", context="bucket")

def ensure_dynamodb_table(table_name):
    client = boto3.client("dynamodb")
    existing_tables = client.list_tables()["TableNames"]

    if table_name not in existing_tables:
        client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "track_genre", "KeyType": "HASH"},
                {"AttributeName": "listen_time", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "track_genre", "AttributeType": "S"},
                {"AttributeName": "listen_time", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        )
        client.get_waiter("table_exists").wait(TableName=table_name)
        log_to_s3(f"Created DynamoDB table: {table_name}", context="dynamodb")

def write_to_dynamodb(df, table_name):
    df_out = df.selectExpr(
        "CAST(track_genre AS STRING) AS track_genre",
        "CAST(listen_time AS STRING) AS listen_time",
        "user_id", "track_name", "duration_ms", "listen_date"
    )

    log_to_s3(f"Writing to DynamoDB table: {table_name}", context="write")
    df_out.write.format("dynamodb")\
        .option("tableName", table_name)\
        .option("writeBatchSize", "25")\
        .mode("append")\
        .save()

def write_to_s3(df, base_path):
    log_to_s3(f"Writing output to S3: {base_path}", context="write")
    df.write.partitionBy("listen_date").mode("overwrite").parquet(base_path)

def archive_latest_data(bucket, source_prefix, archive_prefix_base):
    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
    archive_prefix = f"{archive_prefix_base}{timestamp}/"

    log_to_s3(f"Archiving {source_prefix} to {archive_prefix}", context="archive")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
    archived_files = 0

    if "Contents" in response:
        for obj in response["Contents"]:
            source_key = obj["Key"]
            destination_key = source_key.replace(source_prefix, archive_prefix, 1)
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=destination_key)
            s3.delete_object(Bucket=bucket, Key=source_key)
            archived_files += 1

    log_to_s3(f"Archived {archived_files} files from {source_prefix}", context="archive")

def log_metrics_to_s3(bucket, key, total, valid, dropped):
    s3 = boto3.client("s3")
    log_content = f"timestamp,total_rows,valid_rows,dropped_rows\n{datetime.utcnow()},{total},{valid},{dropped}"
    s3.put_object(Bucket=bucket, Key=key, Body=log_content.encode("utf-8"))
    log_to_s3(f"Validation metrics written to s3://{bucket}/{key}", context="metrics")

def main():
    glue_context, job = init_glue_job()

    validated_df, total_rows, valid_rows, dropped_rows = load_and_validate_data(glue_context)

    ensure_s3_bucket("lab3-presentation-data")
    ensure_dynamodb_table("daily_listens_by_genre")

    # write_to_dynamodb(validated_df, "daily_listens_by_genre")  # Enable if needed
    write_to_s3(validated_df, "s3://lab3-presentation-data/daily_listens/")

    archive_latest_data(
        bucket="lab3-curated",
        source_prefix="transformed-data/latest/",
        archive_prefix_base="transformed-data/archive/"
    )

    log_metrics_to_s3("lab3-presentation-data", "logs/validation_metrics.csv", total_rows, valid_rows, dropped_rows)

    job.commit()
    log_to_s3("Glue job completed successfully", context="main")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_to_s3(f"Job failed due to: {str(e)}", level="ERROR", context="main")
        raise
