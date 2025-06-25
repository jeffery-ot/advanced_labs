import boto3
import random
import time
import traceback

s3_client = boto3.client('s3')
glue_client = boto3.client('glue', region_name='us-east-1')

# Config
BUCKET = "lab3-raw"
PREFIX = "processed-streams/"
GLUE_JOB_NAME = "batch_transform"  #  Glue job name
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def lambda_handler(event, context):
    try:
        print(f"Checking S3 folder: s3://{BUCKET}/{PREFIX}")

        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
        contents = response.get('Contents', [])

        file_keys = [obj['Key'] for obj in contents if not obj['Key'].endswith('/')]

        file_count = len(file_keys)
        print(f" Found {file_count} files.")

        if file_count >= 1:
            print("Triggering Glue job...")
            trigger_glue_with_retries()
        else:
            print("No files found. Glue job not triggered.")

    except Exception as e:
        print("Lambda execution failed:")
        traceback.print_exc()
        raise e


def trigger_glue_with_retries():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME
               
            )
            print(f" Glue Job Started Successfully! JobRunId: {response['JobRunId']}")
            return
        except Exception as e:
            print(f" Glue trigger failed (attempt {attempt} of {MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(" All attempts to start Glue job failed.")
                raise e
