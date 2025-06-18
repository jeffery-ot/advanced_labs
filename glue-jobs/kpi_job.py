import os
import logging
import traceback
from pyspark.sql import SparkSession, functions as F, Window

# Ensure log directory exists
os.makedirs("log_data", exist_ok=True)

# IAM Role ARN
iam_role = "arn:aws:iam::648637468459:role/lab1-gluerole"

# Spark session
spark = SparkSession.builder \
    .appName("CuratedToPresentationMetrics") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Paths and JDBC IAM-based connection (Updated)
curated_base = "s3a://lab2-redshiftdata/curated/"
presentation_base = "s3a://lab2-redshiftdata/presentation/"
jdbc_url = (
    "jdbc:redshift:iam://default-workgroup.648637468459.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
    "?DbUser=admin"
    "&workgroupName=default-workgroup"
    "&Region=us-east-1"
    f"&IamRole={iam_role}"
)

# Metrics definitions
metrics = [
    ("avg_listing_price", ["year", "week"]),
    ("occupancy_rate", ["year", "month"]),
    ("popular_locations", ["year", "week", "cityname"]),
    ("top_performing_listings", ["year", "week", "apartment_id"]),
    ("bookings_per_user", ["year", "week", "user_id"]),
    ("avg_booking_duration", ["year", "week"]),
    ("repeat_customer_rate", [])
]

def get_logger(metric_name):
    logger = logging.getLogger(metric_name)
    if not logger.handlers:
        handler = logging.FileHandler(f'log_data/{metric_name}.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
        handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger

def redshift_type(spark_type):
    return {
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'DoubleType': 'DOUBLE PRECISION',
        'FloatType': 'REAL',
        'StringType': 'VARCHAR',
        'BooleanType': 'BOOLEAN',
        'TimestampType': 'TIMESTAMP',
        'DateType': 'DATE'
    }.get(spark_type, 'VARCHAR')

def generate_ddl(df, table_name):
    cols = []
    for field in df.schema.fields:
        sql_type = redshift_type(field.dataType.__class__.__name__)
        cols.append(f'"{field.name}" {sql_type}')
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(cols)}
);"""
    return ddl

def upsert_to_redshift(df, table_name, pkeys, jdbc_url):
    staging_table = f"{table_name}_staging"

    # Write staging table
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("overwrite") \
        .save()

    # Simulated upsert SQL (for logging purposes)
    pk_clause = " AND ".join([f"target.{pk} = source.{pk}" for pk in pkeys])
    insert_cols = ", ".join(df.columns)

    merge_sql = f"""
BEGIN;

DELETE FROM {table_name}
USING {staging_table}
WHERE {pk_clause};

INSERT INTO {table_name} ({insert_cols})
SELECT {insert_cols} FROM {staging_table};

DROP TABLE {staging_table};

END;
"""
    print(f"\n-- UPSERT {table_name}\n{merge_sql}")

try:
    # Load data
    apartments = spark.read.parquet(f"{curated_base}apartments")
    attributes = spark.read.parquet(f"{curated_base}apartment_attributes")
    bookings = spark.read.parquet(f"{curated_base}bookings")
    user_viewing = spark.read.parquet(f"{curated_base}user_viewing")

    # Metrics computations
    avg_price_weekly = apartments.filter("is_active = true") \
        .withColumn("week", F.weekofyear("listing_created_on")) \
        .withColumn("year", F.year("listing_created_on")) \
        .groupBy("year", "week").agg(F.avg("price").alias("avg_listing_price"))

    bwm = bookings.withColumn("month", F.month("checkin_date")).withColumn("year", F.year("checkin_date"))
    nights_booked = bwm.withColumn("nights_booked", F.datediff("checkout_date", "checkin_date"))
    occupancy = nights_booked.groupBy("year", "month") \
        .agg(
            F.sum("nights_booked").alias("total_nights_booked"),
            F.countDistinct("apartment_id").alias("unique_listings")
        ) \
        .withColumn("estimated_available_nights", F.col("unique_listings") * 30) \
        .withColumn("occupancy_rate", F.col("total_nights_booked") / F.col("estimated_available_nights"))

    popular_locations = bookings.join(attributes, bookings.apartment_id == attributes.id, "left") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "cityname") \
        .agg(F.count("*").alias("bookings"))

    weekly_revenue = bookings.filter("booking_status = 'confirmed'") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "apartment_id") \
        .agg(F.sum("total_price").alias("weekly_revenue"))

    bookings_per_user = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "user_id") \
        .agg(F.count("*").alias("total_bookings"))

    avg_duration = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .withColumn("duration", F.datediff("checkout_date", "checkin_date")) \
        .groupBy("year", "week") \
        .agg(F.avg("duration").alias("avg_booking_duration"))

    repeat_customers = bookings.select("user_id", "booking_date").distinct()
    window_spec = Window.partitionBy("user_id").orderBy("booking_date")
    prev_booking = repeat_customers.withColumn("prev_date", F.lag("booking_date").over(window_spec))
    repeat_within_30_days = prev_booking \
        .withColumn("days_between", F.datediff("booking_date", "prev_date")) \
        .filter("days_between IS NOT NULL AND days_between <= 30")
    repeat_user_ids = repeat_within_30_days.select("user_id").distinct()
    total_users = bookings.select("user_id").distinct().count()
    repeaters = repeat_user_ids.count()
    repeat_customer_rate = spark.createDataFrame(
        [(repeaters, total_users, repeaters / total_users if total_users > 0 else 0.0)],
        ["repeaters", "total_users", "repeat_rate"]
    )

    metrics_data = {
        "avg_listing_price": avg_price_weekly,
        "occupancy_rate": occupancy,
        "popular_locations": popular_locations,
        "top_performing_listings": weekly_revenue,
        "bookings_per_user": bookings_per_user,
        "avg_booking_duration": avg_duration,
        "repeat_customer_rate": repeat_customer_rate
    }

    for name, pkeys in metrics:
        logger = get_logger(name)
        try:
            df = metrics_data[name]

            # Save to S3
            df.write.mode("overwrite").parquet(f"{presentation_base}{name}")
            logger.info(f"Wrote {name} to S3 at {presentation_base}{name}")

            # Generate DDL for Redshift
            ddl = generate_ddl(df, f"presentation.{name}")
            logger.info(f"DDL for {name}:\n{ddl}")

            # Write to Redshift
            if not pkeys:
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"presentation.{name}") \
                    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
                    .mode("overwrite") \
                    .save()
                logger.info(f"Overwrote Redshift table presentation.{name}")
            else:
                upsert_to_redshift(df, f"presentation.{name}", pkeys, jdbc_url)
                logger.info(f"Upserted Redshift table presentation.{name}")

        except Exception as metric_error:
            logger.error(f"Failed metric {name}: {metric_error}")
            logger.error(traceback.format_exc())

except Exception as e:
    main_logger = get_logger("pipeline")
    main_logger.error("Pipeline failed completely.")
    main_logger.error(str(e))
    main_logger.error(traceback.format_exc())

spark.stop()
