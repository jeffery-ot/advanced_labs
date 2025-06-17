import os
import logging
import traceback
from pyspark.sql import SparkSession, functions as F


# Ensure log directory exists
os.makedirs("/opt/spark/log_data", exist_ok=True)

logging.basicConfig(
    filename='log_data/curated_export.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

spark = SparkSession.builder.appName("RawToCuratedSelectedColumns").getOrCreate()

raw_base = "s3a://lab2-extractiondata/raw/"
curated_base = "s3a://lab2-redshiftdata/curated/"
date_format = "dd/MM/yyyy"

for table in ["apartments", "apartment_attributes", "bookings", "user_viewing"]:
    try:
        logging.info(f"Reading from raw S3: {raw_base}{table}")
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

        logging.info(f"Writing curated '{table}' to {curated_base}{table}")
        df.dropna().write.mode("overwrite").parquet(f"{curated_base}{table}")
    except Exception as e:
        logging.error(f"Failed processing '{table}': {e}")
        logging.error(traceback.format_exc())

spark.stop()
