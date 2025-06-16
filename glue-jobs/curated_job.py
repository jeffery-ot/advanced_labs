from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("RawToCurated").getOrCreate()

raw_base = "s3a://lab2-extractiondata/raw/"
curated_base = "s3a://lab2-redshiftdata/curated/"

for table in ["apartments", "apartment_attributes", "bookings", "user_viewing"]:
    df = spark.read.parquet(f"{raw_base}{table}")

    # Cast columns by table
    if table == "apartments":
        df = df \
            .withColumn("id", F.col("id").cast("string")) \
            .withColumn("price", F.col("price").cast("double")) \
            .withColumn("is_active", F.col("is_active").cast("boolean")) \
            .withColumn("listing_created_on", F.to_date("listing_created_on"))

    elif table == "apartment_attributes":
        df = df \
            .withColumn("id", F.col("id").cast("string")) \
            .withColumn("bathrooms", F.col("bathrooms").cast("int")) \
            .withColumn("bedrooms", F.col("bedrooms").cast("int")) \
            .withColumn("pets_allowed", F.col("pets_allowed").cast("boolean")) \
            .withColumn("price_display", F.col("price_display").cast("double")) \
            .withColumn("square_feet", F.col("square_feet").cast("double")) \
            .withColumn("latitude", F.col("latitude").cast("double")) \
            .withColumn("longitude", F.col("longitude").cast("double"))

    elif table == "bookings":
        df = df \
            .withColumn("booking_id", F.col("booking_id").cast("string")) \
            .withColumn("user_id", F.col("user_id").cast("string")) \
            .withColumn("apartment_id", F.col("apartment_id").cast("string")) \
            .withColumn("booking_date", F.to_date("booking_date")) \
            .withColumn("checkin_date", F.to_date("checkin_date")) \
            .withColumn("checkout_date", F.to_date("checkout_date")) \
            .withColumn("total_price", F.col("total_price").cast("double"))

    elif table == "user_viewing":
        df = df \
            .withColumn("user_id", F.col("user_id").cast("string")) \
            .withColumn("apartment_id", F.col("apartment_id").cast("string")) \
            .withColumn("viewed_at", F.to_timestamp("viewed_at")) \
            .withColumn("is_wishlisted", F.col("is_wishlisted").cast("boolean"))

    # Drop rows with any nulls after casting
    df_clean = df.dropna()

    df_clean.write.mode("overwrite").parquet(f"{curated_base}{table}")

spark.stop()
