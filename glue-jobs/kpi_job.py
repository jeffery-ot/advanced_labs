import os
import logging
import traceback
from pyspark.sql import SparkSession, functions as F, Window


# Ensure log directory exists
os.makedirs("/opt/spark/log_data", exist_ok=True)

logging.basicConfig(
    filename='log_data/presentation_export.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

spark = SparkSession.builder.appName("CuratedToPresentationMetrics").getOrCreate()

curated_base = "s3a://lab2-redshiftdata/curated/"
presentation_base = "s3a://lab2-redshiftdata/presentation/"

try:
    apartments = spark.read.parquet(f"{curated_base}apartments")
    attributes = spark.read.parquet(f"{curated_base}apartment_attributes")
    bookings = spark.read.parquet(f"{curated_base}bookings")
    user_viewing = spark.read.parquet(f"{curated_base}user_viewing")

    # 1. Average Listing Price
    avg_price_weekly = apartments.filter("is_active = true") \
        .withColumn("week", F.weekofyear("listing_created_on")) \
        .withColumn("year", F.year("listing_created_on")) \
        .groupBy("year", "week").agg(F.avg("price").alias("avg_listing_price"))
    avg_price_weekly.write.mode("overwrite").parquet(f"{presentation_base}avg_listing_price")

    # 2. Occupancy Rate
    bwm = bookings.withColumn("month", F.month("checkin_date")).withColumn("year", F.year("checkin_date"))
    nights_booked = bwm.withColumn("nights_booked", F.datediff("checkout_date", "checkin_date"))
    occupancy = nights_booked.groupBy("year", "month") \
        .agg(
            F.sum("nights_booked").alias("total_nights_booked"),
            F.countDistinct("apartment_id").alias("unique_listings")
        ) \
        .withColumn("estimated_available_nights", F.col("unique_listings") * 30) \
        .withColumn("occupancy_rate", F.col("total_nights_booked") / F.col("estimated_available_nights"))
    occupancy.write.mode("overwrite").parquet(f"{presentation_base}occupancy_rate")

    # 3. Most Popular Locations
    popular_locations = bookings.join(attributes, bookings.apartment_id == attributes.id, "left") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "cityname") \
        .agg(F.count("*").alias("bookings")) \
        .orderBy("year", "week", F.desc("bookings"))
    popular_locations.write.mode("overwrite").parquet(f"{presentation_base}popular_locations")

    # 4. Weekly Revenue
    weekly_revenue = bookings.filter("booking_status = 'confirmed'") \
        .withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "apartment_id") \
        .agg(F.sum("total_price").alias("weekly_revenue")) \
        .orderBy(F.desc("weekly_revenue"))
    weekly_revenue.write.mode("overwrite").parquet(f"{presentation_base}top_performing_listings")

    # 5. Bookings per User
    bookings_per_user = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .groupBy("year", "week", "user_id") \
        .agg(F.count("*").alias("total_bookings"))
    bookings_per_user.write.mode("overwrite").parquet(f"{presentation_base}bookings_per_user")

    # 6. Avg Booking Duration
    avg_duration = bookings.withColumn("week", F.weekofyear("booking_date")) \
        .withColumn("year", F.year("booking_date")) \
        .withColumn("duration", F.datediff("checkout_date", "checkin_date")) \
        .groupBy("year", "week") \
        .agg(F.avg("duration").alias("avg_booking_duration"))
    avg_duration.write.mode("overwrite").parquet(f"{presentation_base}avg_booking_duration")

    # 7. Repeat Customer Rate
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
    repeat_customer_rate.write.mode("overwrite").parquet(f"{presentation_base}repeat_customer_rate")

except Exception as e:
    logging.error(f" Metric pipeline failed: {e}")
    logging.error(traceback.format_exc())

spark.stop()
