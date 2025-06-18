import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import traceback
from pyspark.sql import functions as F, Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# IAM Role ARN
iam_role = "arn:aws:iam::648637468459:role/lab1-gluerole"

# Paths and JDBC IAM-based connection
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
    ddl_body = ",\n".join(cols)
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{ddl_body}
);"""
    return ddl

try:
    # Load data
    print("Reading curated tables from S3...")
    apartments = spark.read.parquet(f"{curated_base}apartments")
    attributes = spark.read.parquet(f"{curated_base}apartment_attributes")
    bookings   = spark.read.parquet(f"{curated_base}bookings")
    user_viewing = spark.read.parquet(f"{curated_base}user_viewing")

    # Compute metrics
    avg_price_weekly = apartments.filter("is_active = true") \
        .withColumn("week", F.weekofyear("listing_created_on")) \
        .withColumn("year", F.year("listing_created_on")) \
        .groupBy("year","week") \
        .agg(F.avg("price").alias("avg_listing_price"))

    bwm = bookings.withColumn("month", F.month("checkin_date")) \
                  .withColumn("year", F.year("checkin_date"))
    nights_booked = bwm.withColumn("nights_booked", F.datediff("checkout_date","checkin_date"))
    occupancy = nights_booked.groupBy("year","month") \
        .agg(
            F.sum("nights_booked").alias("total_nights_booked"),
            F.countDistinct("apartment_id").alias("unique_listings")
        ) \
        .withColumn("estimated_available_nights", F.col("unique_listings")*30) \
        .withColumn("occupancy_rate", F.col("total_nights_booked")/F.col("estimated_available_nights"))

    popular_locations = bookings.join(attributes, bookings.apartment_id==attributes.id,"left") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year","week","cityname") \
        .agg(F.count("*").alias("bookings"))

    weekly_revenue = bookings.filter("booking_status='confirmed'") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year","week","apartment_id") \
        .agg(F.sum("total_price").alias("weekly_revenue"))

    bookings_per_user = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year","week","user_id") \
        .agg(F.count("*").alias("total_bookings"))

    avg_duration = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .withColumn("duration", F.datediff("checkout_date","checkin_date")) \
        .groupBy("year","week") \
        .agg(F.avg("duration").alias("avg_booking_duration"))

    repeat_users = bookings.select("user_id","booking_date").distinct()
    window_spec = Window.partitionBy("user_id").orderBy("booking_date")
    prev = repeat_users.withColumn("prev_date", F.lag("booking_date").over(window_spec))
    repeat30 = prev.withColumn("days_between", F.datediff("booking_date","prev_date")) \
                   .filter("days_between <= 30")
    repeat_count = repeat30.select("user_id").distinct().count()
    total_count  = bookings.select("user_id").distinct().count()
    repeat_customer_rate = spark.createDataFrame(
        [(repeat_count, total_count, repeat_count/total_count if total_count>0 else 0.0)],
        ["repeaters","total_users","repeat_rate"]
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

    # Write each metric with overwrite
    for name, _ in metrics:
        df = metrics_data[name]
        print(f"\nProcessing metric: {name}")

        # Save to S3
        df.write.mode("overwrite").parquet(f"{presentation_base}{name}")
        print(f"Saved {name} to S3 at {presentation_base}{name}")

        # Generate & show DDL
        ddl = generate_ddl(df, f"presentation.{name}")
        print(f"DDL for {name}:\n{ddl}")

        # Overwrite in Redshift
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", f"presentation.{name}") \
          .option("driver", "com.amazon.redshift.jdbc42.Driver") \
          .mode("overwrite") \
          .save()
        print(f"Overwrote Redshift table presentation.{name}")

except Exception as e:
    print("Pipeline failed completely.")
    print(str(e))
    print(traceback.format_exc())

finally:
    job.commit()
    