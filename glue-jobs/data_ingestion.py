import os
import logging
import traceback
from pyspark.sql import SparkSession


# Ensure log directory exists
os.makedirs("/opt/spark/log_data", exist_ok=True)

# Setup logging
logging.basicConfig(
    filename='log_data/ingestion_export.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

spark = SparkSession.builder.appName("DataIngestionAndExport").getOrCreate()

jdbc_url = "jdbc:mysql://mysql-5.7:3306/mysql_dev"
jdbc_write_properties = {"user": "mysql_user", "password": "Amalitech", "driver": "com.mysql.cj.jdbc.Driver"}
jdbc_read_properties = {"user": "mysql_user", "password": "Amalitech", "driver": "com.mysql.cj.jdbc.Driver", "fetchsize": "50000"}

files_and_tables = {
    "/opt/data/apartments.csv": "apartments",
    "/opt/data/apartment_attributes.csv": "apartment_attributes",
    "/opt/data/bookings.csv": "bookings",
    "/opt/data/user_viewing.csv": "user_viewing"
}

s3_output_base = "s3a://lab2-extractiondata/raw/"

for file_path, table_name in files_and_tables.items():
    try:
        df = spark.read.option("header", True).csv(file_path)
        logging.info(f"Writing {file_path} to MySQL table '{table_name}'")
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=jdbc_write_properties)
    except Exception as e:
        logging.error(f"Failed to load {file_path} into {table_name}: {e}")
        logging.error(traceback.format_exc())

for _, table_name in files_and_tables.items():
    try:
        logging.info(f"Reading MySQL table '{table_name}'")
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_read_properties)
        logging.info(f" Previewing data from '{table_name}'")
        df.show(10, truncate=False)
        s3_path = f"{s3_output_base}{table_name}"
        logging.info(f"Writing '{table_name}' to {s3_path} as Parquet")
        df.write.mode("append").parquet(s3_path)
    except Exception as e:
        logging.error(f"Failed to export {table_name} to S3: {e}")
        logging.error(traceback.format_exc())

spark.stop()
