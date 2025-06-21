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
        "songs": ["track_id", "track_name", "popularity","duration_ms", "track_genre"],
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
    print(f"Reading {name} from {source['path']} ({source['format']})")
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
            print(f"Loaded: {name}")
        except Exception as e:
            print(f"Error loading {name}: {e}")
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
    print(f"[{dataset_name}] Validation passed.")
    return DynamicFrame.fromDF(df.select(*required_columns), dyf.glue_ctx, dataset_name)

# ----------------------------------------
# Parquet column casting
# ----------------------------------------

def cast_stream_columns(glue_context, stream_dyf):
    df = stream_dyf.toDF()

    # Cast user_id to Integer and listen_time to Timestamp
    df = (
        df
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("listen_time", col("listen_time").cast("timestamp"))
        .filter(col("user_id").isNotNull() & col("track_id").isNotNull())
    )

    return DynamicFrame.fromDF(df, glue_context, "stream")

# ----------------------------------------
# Writer
# ----------------------------------------

def write_to_s3(glue_context, dynamic_frame, target_uri: str):
    output_path = f"{target_uri.rstrip('/')}/latest/"
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path, "partitionKeys": ["track_genre"]},
        format="parquet"
    )
    print(f"Data written to {output_path}")

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

    print(f"Archiving from s3://{src_bucket}/{src_prefix} to s3://{dest_bucket}/{dest_prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    archived_files = 0

    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dest_key = dest_prefix + src_key[len(src_prefix):]
            s3.copy_object(Bucket=dest_bucket, CopySource={'Bucket': src_bucket, 'Key': src_key}, Key=dest_key)
            s3.delete_object(Bucket=src_bucket, Key=src_key)
            archived_files += 1

    print(f"Archive complete. Files archived: {archived_files}")

# ----------------------------------------
# Main Logic
# ----------------------------------------

def main():
    glue_context, job = init_glue_job()
    all_data = load_all_data(glue_context)

    all_data["stream"] = cast_stream_columns(glue_context, all_data["stream"])

    required_cols = get_required_columns()
    validated = {
        name: validate_and_select(dyf, required_cols[name], name)
        for name, dyf in all_data.items()
    }

    stream_df = validated["stream"].toDF()
    users_df = validated["users"].toDF()
    songs_df = validated["songs"].toDF()

    stream_df = stream_df.withColumn("track_id", trim("track_id"))
    songs_df = songs_df.withColumn("track_id", trim("track_id"))

    print("Sample stream_df:")
    stream_df.limit(5).show()

    print("Sample songs_df:")
    songs_df.limit(5).show()

    joined_df = (
        stream_df
        .join(songs_df, on="track_id", how="inner")
        .join(users_df, on="user_id", how="inner")
    )

    print(f"Joined records sample:")
    joined_df.select(
        "track_id", "user_id", "listen_time", "user_name",
        "track_name", "popularity", "track_genre"
    ).show(10, truncate=False)

    final_dyf = DynamicFrame.fromDF(joined_df, glue_context, "final_output")
    write_to_s3(glue_context, final_dyf, "s3://lab3-curated/transformed-data/")

    archive_data("s3://lab3-raw/processed-streams/", "s3://lab3-raw/archives/")

    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Job failed due to: {str(e)}", file=sys.stderr)
        raise
