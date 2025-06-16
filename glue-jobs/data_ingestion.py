from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataIngestion") \
    .getOrCreate()

# JDBC connection info
jdbc_url = "jdbc:mysql://mysql-5.7:3306/mysql_dev"
jdbc_properties = {
    "user": "mysql_user",
    "password": "Amalitech",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Absolute container paths
files_and_tables = {
    "/opt/data/apartments.csv": "apartments",
    "/opt/data/apartment_attributes.csv": "apartment_attributes",
    "/opt/data/bookings.csv": "bookings",
    "/opt/data/user_viewing.csv": "user_viewing"
}

for file_path, table_name in files_and_tables.items():
    try:
        df = spark.read.option("header", True).csv(file_path)
        print(f"Writing {file_path} to MySQL table {table_name}")
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=jdbc_properties)
    except Exception as e:
        print(f"Failed to load {file_path} into {table_name}: {e}")

spark.stop()
