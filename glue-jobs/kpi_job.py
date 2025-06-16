from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("BusinessMetrics").getOrCreate()

# Load curated parquet with casts
apartments = spark.read.parquet("s3a://lab2-redshiftdata/curated/apartments") \
    .withColumn("id", F.col("id").cast("string")) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("is_active", F.col("is_active").cast("boolean")) \
    .withColumn("listing_created_on", F.to_date(F.col("listing_created_on")))

apartment_attributes = spark.read.parquet("s3a://lab2-redshiftdata/curated/apartment_attributes") \
    .withColumn("id", F.col("id").cast("string")) \
    .withColumn("bathrooms", F.col("bathrooms").cast("int")) \
    .withColumn("bedrooms", F.col("bedrooms").cast("int")) \
    .withColumn("pets_allowed", F.col("pets_allowed").cast("boolean")) \
    .withColumn("price_display", F.col("price_display").cast("double")) \
    .withColumn("square_feet", F.col("square_feet").cast("double")) \
    .withColumn("latitude", F.col("latitude").cast("double")) \
    .withColumn("longitude", F.col("longitude").cast("double")) \
    .withColumnRenamed("cityname", "cityname")

bookings = spark.read.parquet("s3a://lab2-redshiftdata/curated/bookings") \
    .withColumn("booking_id", F.col("booking_id").cast("string")) \
    .withColumn("user_id", F.col("user_id").cast("string")) \
    .withColumn("apartment_id", F.col("apartment_id").cast("string")) \
    .withColumn("booking_date", F.to_date(F.col("booking_date"))) \
    .withColumn("checkin_date", F.to_date(F.col("checkin_date"))) \
    .withColumn("checkout_date", F.to_date(F.col("checkout_date"))) \
    .withColumn("total_price", F.col("total_price").cast("double")) \
    .withColumn("booking_status", F.col("booking_status").cast("string"))

user_viewing = spark.read.parquet("s3a://lab2-redshiftdata/curated/user_viewing") \
    .withColumn("user_id", F.col("user_id").cast("string")) \
    .withColumn("apartment_id", F.col("apartment_id").cast("string")) \
    .withColumn("viewed_at", F.to_timestamp(F.col("viewed_at"))) \
    .withColumn("is_wishlisted", F.col("is_wishlisted").cast("boolean"))


# Filter active apartments and confirmed bookings
active_apartments = apartments.filter(F.col("is_active") == True)
confirmed_bookings = bookings.filter(F.col("booking_status") == "confirmed")


# 1a) Average Listing Price per week
avg_price_weekly = confirmed_bookings \
    .join(active_apartments, confirmed_bookings.apartment_id == active_apartments.id) \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week") \
    .agg(F.avg("total_price").alias("avg_listing_price"))

avg_price_weekly.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/avg_listing_price")


# 1b) Occupancy Rate per month
booked_nights = confirmed_bookings \
    .withColumn("nights", F.datediff("checkout_date", "checkin_date")) \
    .groupBy(F.year("booking_date").alias("year"), F.month("booking_date").alias("month"), "apartment_id") \
    .agg(F.sum("nights").alias("booked_nights"))

days_in_month = booked_nights.select("year", "month").distinct() \
    .withColumn("days_in_month", F.dayofmonth(F.last_day(F.concat_ws("-", "year", "month", F.lit("01")))))

occupancy = booked_nights.join(days_in_month, ["year", "month"]) \
    .withColumn("occupancy_rate", F.col("booked_nights") / F.col("days_in_month")) \
    .select("year", "month", "apartment_id", "occupancy_rate")

occupancy.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/occupancy_rate")


# 1c) Most Popular Locations per week
popular_locations = confirmed_bookings \
    .join(apartment_attributes, confirmed_bookings.apartment_id == apartment_attributes.id) \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week", "cityname") \
    .count() \
    .orderBy(F.desc("count"))

popular_locations.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/popular_locations")


# 1d) Top Performing Listings by revenue per week
top_listings = confirmed_bookings \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week", "apartment_id") \
    .agg(F.sum("total_price").alias("total_revenue")) \
    .orderBy(F.desc("total_revenue"))

top_listings.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/top_listings")


# 2a) Total Bookings per user per week
bookings_per_user = confirmed_bookings \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week", "user_id") \
    .count() \
    .withColumnRenamed("count", "total_bookings")

bookings_per_user.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/bookings_per_user")


# 2b) Average Booking Duration (days) per week
avg_duration = confirmed_bookings \
    .withColumn("duration", F.datediff("checkout_date", "checkin_date")) \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week") \
    .agg(F.avg("duration").alias("avg_booking_duration"))

avg_duration.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/avg_booking_duration")


# 2c) Repeat Customer Rate within rolling 30 days
window_30d = Window.partitionBy("user_id").orderBy(F.col("booking_date").cast("long")).rangeBetween(-30 * 86400, 0)

repeat_rate = confirmed_bookings \
    .withColumn("bookings_30d", F.count("booking_date").over(window_30d)) \
    .withColumn("is_repeat", F.when(F.col("bookings_30d") > 1, 1).otherwise(0)) \
    .withColumn("week", F.weekofyear("booking_date")) \
    .groupBy("week") \
    .agg((F.sum("is_repeat") / F.countDistinct("user_id")).alias("repeat_customer_rate"))

repeat_rate.write.mode("overwrite").parquet("s3a://lab2-redshiftdata/presentation/repeat_customer_rate")

spark.stop()
