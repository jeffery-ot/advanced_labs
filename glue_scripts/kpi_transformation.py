import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
import boto3

# ----------------------------------------
# Glue Job Initialization
# ----------------------------------------

def init_glue_job(job_name: str = "normalized-genre-data-job"):
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(job_name, {})
    return glue_context, job

# ----------------------------------------
# Load, Cast, and Validate Data
# ----------------------------------------

def load_and_validate_data(glue_context):
    spark = glue_context.spark_session
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

    print(f" Total rows read: {total_rows}")
    print(f" Valid rows: {valid_rows}")
    print(f" Dropped rows due to validation: {dropped_rows}")

    return validated_df, total_rows, valid_rows, dropped_rows

# ----------------------------------------
# Ensure S3 Bucket Exists
# ----------------------------------------

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)

# ----------------------------------------
# Ensure DynamoDB Table Exists
# ----------------------------------------

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

# ----------------------------------------
# Write to DynamoDB using boto3
# ----------------------------------------

def write_to_dynamodb(df, table_name):
    import boto3
    import pandas as pd

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Convert to Pandas (small data only)
    records = df.select(
        "track_genre", "listen_time", "user_id", "track_name", "duration_ms", "listen_date"
    ).toPandas().to_dict(orient="records")

    with table.batch_writer(overwrite_by_pkeys=["track_genre", "listen_time"]) as batch:
        for item in records:
            # Ensure proper types
            item["track_genre"] = str(item.get("track_genre", ""))
            item["listen_time"] = item.get("listen_time")
            if isinstance(item["listen_time"], datetime):
                item["listen_time"] = item["listen_time"].isoformat()
            elif item["listen_time"] is not None:
                item["listen_time"] = str(item["listen_time"])
            else:
                continue  # skip item if listen_time is missing

            item["user_id"] = str(item.get("user_id", ""))
            item["track_name"] = str(item.get("track_name", ""))
            item["duration_ms"] = int(item.get("duration_ms", 0))
            listen_date = item.get("listen_date")
            item["listen_date"] = (
                listen_date.isoformat() if isinstance(listen_date, datetime) else str(listen_date)
            )

            batch.put_item(Item=item)

# ----------------------------------------
# Write to S3
# ----------------------------------------

def write_to_s3(df, base_path):
    df.write.partitionBy("listen_date").mode("overwrite").parquet(base_path)

# ----------------------------------------
# Archive Latest Data
# ----------------------------------------

def archive_latest_data(bucket, source_prefix, archive_prefix_base):
    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
    archive_prefix = f"{archive_prefix_base}{timestamp}/"

    response = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            source_key = obj["Key"]
            destination_key = source_key.replace(source_prefix, archive_prefix, 1)

            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=destination_key)
            s3.delete_object(Bucket=bucket, Key=source_key)

# ----------------------------------------
# Log Validation Metrics to S3
# ----------------------------------------

def log_metrics_to_s3(bucket, key, total, valid, dropped):
    s3 = boto3.client("s3")
    log_content = f"timestamp,total_rows,valid_rows,dropped_rows\n{datetime.utcnow()},{total},{valid},{dropped}"
    s3.put_object(Bucket=bucket, Key=key, Body=log_content.encode("utf-8"))

# ----------------------------------------
# Main
# ----------------------------------------

def main():
    glue_context, job = init_glue_job()

    validated_df, total_rows, valid_rows, dropped_rows = load_and_validate_data(glue_context)

    ensure_s3_bucket("lab3-presentation-data")
    ensure_dynamodb_table("daily_listens_by_genre")

    write_to_dynamodb(validated_df, "daily_listens_by_genre")
    write_to_s3(validated_df, "s3://lab3-presentation-data/daily_listens/")

    archive_latest_data(
        bucket="lab3-curated",
        source_prefix="transformed-data/latest/",
        archive_prefix_base="transformed-data/archive/"
    )

    log_metrics_to_s3("lab3-presentation-data", "logs/validation_metrics.csv", total_rows, valid_rows, dropped_rows)

    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Job failed: {e}", file=sys.stderr)
        raise
