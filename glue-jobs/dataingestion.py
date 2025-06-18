import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import traceback
from pyspark.sql import functions as F

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_base = "s3a://lab2-extractiondata/raw/"
curated_base = "s3a://lab2-redshiftdata/curated/"
date_format = "dd/MM/yyyy"

for table in ["apartments", "apartment_attributes", "bookings", "user_viewing"]:
    try:
        print(f"Reading from raw S3: {raw_base}{table}")
        df = spark.read.parquet(f"{raw_base}{table}")

        if table == "apartments":
            df = df.select(
                F.col("id").cast("string"),
                F.col("price").cast("double"),
                F.col("currency").cast("string"),
                F.col("is_active").cast("boolean"),
                F.to_date(F.col("listing_created_on"), date_format).alias("listing_created_on")
            )
        elif table == "apartment_attributes":
            df = df.select("id", "category", "cityname")
        elif table == "bookings":
            df = df.select(
                "booking_id", "user_id", "apartment_id",
                F.to_date("booking_date", date_format).alias("booking_date"),
                F.to_date("checkin_date", date_format).alias("checkin_date"),
                F.to_date("checkout_date", date_format).alias("checkout_date"),
                F.col("total_price").cast("double"),
                "currency", "booking_status"
            )
        elif table == "user_viewing":
            df = df.select("user_id", "apartment_id")

        print(f"Writing curated '{table}' to {curated_base}{table}")
        df.dropna().write.mode("overwrite").parquet(f"{curated_base}{table}")

    except Exception as e:
        print(f"Failed processing '{table}': {e}")
        print(traceback.format_exc())

job.commit()