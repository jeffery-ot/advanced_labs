from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataIngestionAndExport") \
    .getOrCreate()

# JDBC MySQL connection info
jdbc_url = "jdbc:mysql://mysql-5.7:3306/mysql_dev"

jdbc_write_properties = {
    "user": "mysql_user",
    "password": "Amalitech",
    "driver": "com.mysql.cj.jdbc.Driver"
}

jdbc_read_properties = {
    "user": "mysql_user",
    "password": "Amalitech",
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": "50000"  # Set fetchsize here for reading/exporting
}

# Absolute paths inside container
files_and_tables = {
    "/opt/data/apartments.csv": "apartments",
    "/opt/data/apartment_attributes.csv": "apartment_attributes",
    "/opt/data/bookings.csv": "bookings",
    "/opt/data/user_viewing.csv": "user_viewing"
}

# S3 destination base path
s3_output_base = "s3a://lab2-extractiondata/raw/"

# Step 1: Load CSVs into MySQL
for file_path, table_name in files_and_tables.items():
    try:
        df = spark.read.option("header", True).csv(file_path)
        print(f"Writing {file_path} to MySQL table '{table_name}'")
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=jdbc_write_properties)
    except Exception as e:
        print(f"Failed to load {file_path} into {table_name}: {e}")

# Step 2: Export from MySQL to S3 as Parquet (with fetchsize)
for _, table_name in files_and_tables.items():
    try:
        print(f"Reading MySQL table '{table_name}' with fetchsize")
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_read_properties)

        s3_path = f"{s3_output_base}{table_name}"
        print(f"Writing '{table_name}' to {s3_path} as Parquet")
        df.write.mode("append").parquet(s3_path)
    except Exception as e:
        print(f"Failed to export {table_name} to S3: {e}")

spark.stop()
