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
# Load Transformed Data
# ----------------------------------------

def load_transformed_data(glue_context):
    df = glue_context.spark_session.read.parquet("s3://lab3-curated/transformed-data/latest/")
    return df.withColumn("listen_date", to_date("listen_time"))

# ----------------------------------------
# Ensure S3 Bucket Exists
# ----------------------------------------

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)

# ----------------------------------------
# Write Normalized Data to S3
# ----------------------------------------

def write_to_s3(df, base_path):
    df.write.partitionBy("listen_date").mode("overwrite").parquet(base_path)

# ----------------------------------------
# Archive the latest/ data
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

            # Copy to archive
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=destination_key)

            # Delete original
            s3.delete_object(Bucket=bucket, Key=source_key)

# ----------------------------------------
# Main
# ----------------------------------------

def main():
    glue_context, job = init_glue_job()

    df = load_transformed_data(glue_context)

    normalized_df = df.select(
        "user_id",
        "track_name",
        "track_genre",
        "duration_ms",
        "listen_time",
        "listen_date"
    )

    ensure_s3_bucket("lab3-presentation")

    write_to_s3(normalized_df, "s3://lab3-presentation-data/daily_listens/")

    # Archive raw data
    archive_latest_data(
        bucket="lab3-curated",
        source_prefix="transformed-data/latest/",
        archive_prefix_base="transformed-data/archive/"
    )

    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Job failed: {e}", file=sys.stderr)
        raise
