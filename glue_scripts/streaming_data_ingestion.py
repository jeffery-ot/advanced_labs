import sys
import random
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set your S3 bucket paths here
source_bucket = "s3://lab3-raw/streams/"
output_bucket = "s3://lab3-raw/processed-streams/"


def init_glue_job():
    """
    Initialize Glue job context and Spark session.
    Returns glue_context and job instance.
    """
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init("hardcoded-ingestion-job", {})  # Hardcoded job name for local/dev
    return glue_context, job

def discover_csv_files_in_s3(glue_context, s3_uri: str) -> list:
    """
    Discover all CSV files in the provided S3 URI using Spark read API.
    """
    spark = glue_context.spark_session

 
    try:
        df = spark.read.csv(s3_uri + "*.csv", header=True, inferSchema=True)
        files = df.inputFiles()
        return files
    except Exception as e:
        raise FileNotFoundError(f"No CSV files found at {s3_uri} or unable to list: {e}")



def select_random_files(files: list) -> list:
    """
    Randomly select a subset of CSV files from the provided list.
    At least one file will be selected.
    """
    k = random.randint(1, len(files))
    return random.sample(files, k)


def read_csv_as_dynamic_frame(glue_context, file_path: str):
    """
    Read a single CSV file from S3 as a Glue DynamicFrame.
    """
    return glue_context.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [file_path]},
        transformation_ctx="read_stream_csv"
    )


def write_to_s3(glue_context, dynamic_frame, target_uri: str):
    """
    Write the given DynamicFrame to the target S3 location in Parquet format.
    A timestamped folder will be used to avoid overwriting.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"{target_uri.rstrip('/')}/batch_{timestamp}/"

    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )

    print(f"Data written to: {output_path}")


def main():
    glue_context, job = init_glue_job()

    try:
        available_files = discover_csv_files_in_s3(glue_context, source_bucket)

        if not available_files:
            raise FileNotFoundError(f"No CSV files found at {source_bucket}")

        selected_files = select_random_files(available_files)
        print(f"Selected files for ingestion: {selected_files}")

        for file_path in selected_files:
            print(f"Reading file: {file_path}")
            df = read_csv_as_dynamic_frame(glue_context, file_path)
            
              # Show a preview of the DynamicFrame as a Spark DataFrame
            print("Data preview:")
            df.toDF().show(10, truncate=False)  # Show top 10 rows without truncating columns

            write_to_s3(glue_context, df, output_bucket)

    except Exception as e:
        print(f"Error: {str(e)}")
        job.commit()
        sys.exit(1)

    job.commit()


if __name__ == "__main__":
    main()
