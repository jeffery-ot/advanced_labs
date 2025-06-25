import sys
from datetime import datetime
import boto3
from pyspark.sql.functions import trim, col
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType,
    BooleanType, DoubleType, DateType
)
from helper_function import log_to_s3
import argparse

# ----------------------------------------
# Schema Definitions
# ----------------------------------------

def get_schemas():
    return {
        "users": StructType([
            StructField("user_id", IntegerType(), True),
            StructField("user_name", StringType(), True),
            StructField("user_age", IntegerType(), True),
            StructField("user_country", StringType(), True),
            StructField("created_at", DateType(), True)
        ]),
        "songs": StructType([
            StructField("id", IntegerType(), True),
            StructField("track_id", StringType(), True),
            StructField("artists", StringType(), True),
            StructField("album_name", StringType(), True),
            StructField("track_name", StringType(), True),
            StructField("popularity", IntegerType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("explicit", BooleanType(), True),
            StructField("danceability", DoubleType(), True),
            StructField("energy", DoubleType(), True),
            StructField("key", IntegerType(), True),
            StructField("loudness", DoubleType(), True),
            StructField("mode", IntegerType(), True),
            StructField("speechiness", DoubleType(), True),
            StructField("acousticness", DoubleType(), True),
            StructField("instrumentalness", DoubleType(), True),
            StructField("liveness", DoubleType(), True),
            StructField("valence", DoubleType(), True),
            StructField("tempo", DoubleType(), True),
            StructField("time_signature", IntegerType(), True),
            StructField("track_genre", StringType(), True)
        ]),
        "stream": StructType([
            StructField("user_id", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("listen_time", StringType(), True)
        ])
    }

# ----------------------------------------
# Data Source Config
# ----------------------------------------

def get_data_sources():
    return {
        "users": {
            "path": "s3://lab3-raw/users/users.csv",
            "format": "csv"
        },
        "songs": {
            "path": "s3://lab3-raw/songs/songs.csv",
            "format": "csv"
        },
        "stream": {
            "path": "s3://lab3-raw/processed-streams/",
            "format": "parquet"
        }
    }

def get_required_columns():
    return {
        "users": ["user_id", "user_name"],
        "songs": ["track_id", "track_name", "popularity", "duration_ms", "track_genre"],
        "stream": ["user_id", "track_id", "listen_time"]
    }

# ----------------------------------------
# Job Initialization
# ----------------------------------------

def init_glue_job(job_name: str = "transformation-job"):
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(job_name, {})
    return glue_context, job

# ----------------------------------------
# Data Loader
# ----------------------------------------

def load_dataset(glue_context, name, schema, source):
    log_to_s3(f"Reading {name} from {source['path']} ({source['format']})", context="load")
    reader = glue_context.spark_session.read

    if source["format"] == "csv":
        df = reader.format("csv") \
            .option("header", True) \
            .option("mode", "PERMISSIVE") \
            .option("multiLine", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("badRecordsPath", f"s3://lab3-raw/bad-records/{name}/") \
            .option("dateFormat", "yyyy-MM-dd") \
            .schema(schema) \
            .load(source["path"])
    elif source["format"] == "parquet":
        df = reader.format("parquet") \
            .option("recursiveFileLookup", "true") \
            .schema(schema) \
            .load(source["path"])
    else:
        raise ValueError(f"Unsupported format: {source['format']}")

    return DynamicFrame.fromDF(df, glue_context, name)

def load_all_data(glue_context):
    schemas = get_schemas()
    sources = get_data_sources()
    data = {}
    for name in sources:
        try:
            data[name] = load_dataset(glue_context, name, schemas[name], sources[name])
            log_to_s3(f"Loaded dataset: {name}", context="load")
        except Exception as e:
            log_to_s3(f"Error loading dataset '{name}': {e}", level="ERROR", context="load")
    return data

# ----------------------------------------
# Validation
# ----------------------------------------

def validate_and_select(dyf: DynamicFrame, required_columns: list, dataset_name: str):
    df = dyf.toDF()
    actual_columns = set(df.columns)
    missing = set(required_columns) - actual_columns
    if missing:
        raise ValueError(f"[{dataset_name}] Missing required columns: {', '.join(missing)}")
    log_to_s3(f"[{dataset_name}] Validation passed.", context="validate")
    return DynamicFrame.fromDF(df.select(*required_columns), dyf.glue_ctx, dataset_name)

# ----------------------------------------
# Parquet column casting
# ----------------------------------------

def cast_stream_columns(glue_context, stream_dyf):
    df = stream_dyf.toDF()
    df = (
        df
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("listen_time", col("listen_time").cast("timestamp"))
        .filter(col("user_id").isNotNull() & col("track_id").isNotNull())
    )
    log_to_s3("Casting and cleaning stream data", context="transform")
    return DynamicFrame.fromDF(df, glue_context, "stream")

# ----------------------------------------
# Writer
# ----------------------------------------

def write_to_s3(glue_context, dynamic_frame, target_uri: str):
    output_path = f"{target_uri.rstrip('/')}/latest/"
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet",
        format_options={"compression": "snappy"}
    )
    log_to_s3(f"Data written to {output_path} using Snappy compression", context="write")

# ----------------------------------------
# Archiver
# ----------------------------------------

def archive_data(source_uri: str, dest_uri: str):
    s3 = boto3.client("s3")

    def parse_s3_uri(uri):
        if not uri.startswith("s3://"):
            raise ValueError("URI must start with s3://")
        parts = uri[5:].split("/", 1)
        return parts[0], parts[1].rstrip("/") + "/"

    src_bucket, src_prefix = parse_s3_uri(source_uri)
    dest_bucket, dest_prefix = parse_s3_uri(dest_uri)

    log_to_s3(f"Archiving from s3://{src_bucket}/{src_prefix} to s3://{dest_bucket}/{dest_prefix}", context="archive")
    paginator = s3.get_paginator("list_objects_v2")
    archived_files = 0

    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dest_key = dest_prefix + src_key[len(src_prefix):]
            s3.copy_object(Bucket=dest_bucket, CopySource={'Bucket': src_bucket, 'Key': src_key}, Key=dest_key)
            s3.delete_object(Bucket=src_bucket, Key=src_key)
            archived_files += 1

    log_to_s3(f"Archive complete. Files archived: {archived_files}", context="archive")

# ----------------------------------------
# Main Logic
# ----------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["load", "transform"], help="Run specific step")
    parser.add_argument("--list", action="store_true", help="List all steps")
    args = parser.parse_args()

    if args.list:
        print("Available steps: load, transform")
        sys.exit(0)

    log_to_s3("Starting Glue ETL job", context="main")
    glue_context, job = init_glue_job()
    all_data = load_all_data(glue_context)

    if args.step == "load":
        log_to_s3("Loaded datasets only (load step completed)", context="main")
        job.commit()
        return

    all_data["stream"] = cast_stream_columns(glue_context, all_data["stream"])
    required_cols = get_required_columns()
    validated = {name: validate_and_select(dyf, required_cols[name], name)
                 for name, dyf in all_data.items()}

    stream_df = validated["stream"].toDF()
    users_df = validated["users"].toDF()
    songs_df = validated["songs"].toDF()

    stream_df = stream_df.withColumn("track_id", trim("track_id"))
    songs_df = songs_df.withColumn("track_id", trim("track_id"))

    log_to_s3("Joining stream, users, and songs datasets", context="transform")
    joined_df = (
        stream_df.join(songs_df, "track_id").join(users_df, "user_id")
    )

    final_dyf = DynamicFrame.fromDF(joined_df, glue_context, "final_output")
    write_to_s3(glue_context, final_dyf, "s3://lab3-curated/transformed-data/")
    archive_data("s3://lab3-raw/processed-streams/", "s3://lab3-raw/archives/")

    job.commit()
    log_to_s3("Glue ETL job completed successfully", context="main")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_to_s3(f"Job failed due to: {str(e)}", level="ERROR", context="main")
        raise
