import sys 
import random
import time
import os
from datetime import datetime
from awsglue.utils  import getResolvedOptions
from glue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job


#Glue job setup
def init_glue_job():
    """Initialize Spark, GlueContext, and Job."""
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    return glue_context, spark, job

#local dir where stream files are located inside the container
streams_dir = "/home/glue_user/workspace/data/streams"

def discover_csv_files(streams_dir: str) -> list:

    if not os.path.exists(streams_dir):
        raise FileNotFoundError(f"Directory does not exist: {streams_dir}")
    
    return [
            f"file://{os.path.join(streams_dir, f)}" 
            for f in os.listdir(streams_dir)
            if f.endswith(".csv")
            ]

def select_random_file(files: list) -> list:
    return random.choice(files)

def read_csv_as_dynamic_frame(glue_context, file_path: str):
    return glue_context.create_dynamic_frame.from_options(

        format_options ={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [file_path]},
        transformation_ctx="stream_data"
    )


def write_to_s3(glue_context, dynamic_frame, bucket_name: str, prefix: str):

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"s3://{bucket_name}/{prefix}/batch_{timestamp}/"

    glue_context.write_dynamic_frame.from_options(

        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )



def main():
    # Configuration
    local_stream_dir = "/home/glue_user/workspace/data/streams"
    s3_bucket = "s3://lab3-curated/processed-streams/"
    s3_prefix = "streaming-data"

    # Glue setup
    glue_context, _, job = init_glue_job()

    try:
        # Discover and ingest a stream
        stream_files = discover_csv_files(local_stream_dir)
        if not stream_files:
            print("No stream files found.")
            job.commit()
            sys.exit(0)

        selected_file = select_random_file(stream_files)
        print(f"Selected stream file: {selected_file}")

        df = read_csv_as_dynamic_frame(glue_context, selected_file)
        write_to_s3(glue_context, df, s3_bucket, s3_prefix)

    except Exception as e:
        print(f"Error: {str(e)}")
        job.commit()
        sys.exit(1)

    job.commit()


# Entry point
if __name__ == "__main__":
    main()

