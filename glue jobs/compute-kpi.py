import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, count, countDistinct, lit, row_number, hour
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

# Init
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read parquet file from S3
dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://lab1-curateddata/mss-transformed/"], "recurse": True},
    transformation_ctx="dyf"
)

# Step 2: Convert to Spark DataFrame
df = dyf.toDF()
df = df.withColumn("listen_hour", hour(col("listen_time")))

# Step 3: Genre-Level KPIs
genre_metrics = df.groupBy("genre").agg(
    count("*").alias("genre_listen_count"),
    avg("duration_ms").alias("genre_avg_duration"),
    avg("popularity").alias("genre_popularity_index")
)

window_genre = Window.partitionBy("genre").orderBy(col("popularity").desc())
genre_top = df.withColumn("rn", row_number().over(window_genre)) \
              .filter(col("rn") == 1) \
              .select("genre", col("track_name").alias("top_track"), col("artists").alias("top_artist"))

genre_kpis = genre_metrics.join(genre_top, on="genre", how="left") \
    .withColumn("kpi_type", lit("genre")) \
    .withColumn("group_by", col("genre")) \
    .select(
        "kpi_type", "group_by",
        col("genre_listen_count").alias("genre_listen_count"),
        col("genre_avg_duration").alias("genre_avg_duration"),
        col("genre_popularity_index").alias("genre_popularity_index"),
        col("top_track"),
        col("top_artist")
    )

# Step 4: Hourly KPIs
hourly_metrics = df.groupBy("listen_hour").agg(
    countDistinct("user_id").alias("unique_listeners"),
    (countDistinct("track_id") / count("*")).alias("track_diversity_index")
)

artist_count = df.groupBy("listen_hour", "artists").agg(count("*").alias("artist_listens"))

window_hour = Window.partitionBy("listen_hour").orderBy(col("artist_listens").desc())

top_artist_hour = artist_count.withColumn("rn", row_number().over(window_hour)) \
                              .filter(col("rn") == 1) \
                              .select("listen_hour", col("artists").alias("top_artist"))

hourly_kpis = hourly_metrics.join(top_artist_hour, on="listen_hour", how="left") \
    .withColumn("kpi_type", lit("hourly")) \
    .withColumn("group_by", col("listen_hour").cast("string")) \
    .select(
        "kpi_type", "group_by",
        col("unique_listeners").alias("genre_listen_count"),
        col("track_diversity_index").alias("genre_avg_duration"),
        lit(None).cast("double").alias("genre_popularity_index"),
        lit(None).cast("string").alias("top_track"),
        col("top_artist")
    )

# Step 5: Combine KPIs
final_df = genre_kpis.unionByName(hourly_kpis, allowMissingColumns=True)

# Step 6: Convert to DynamicFrame
final_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_dyf")

# Step 7: Write to S3 (optional audit/debug)
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://lab1-analytics/kpi_output/",
        "partitionKeys": ["kpi_type"]
    }
)

# Step 8: UPSERT into Redshift via staging
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=final_dyf,
    catalog_connection="Redshift connection",
    connection_options={
        "dbtable": "public.kpi_staging",
        "database": "dev",
        "preactions": """
            CREATE TABLE IF NOT EXISTS public.kpi_staging (
                kpi_type VARCHAR,
                group_by VARCHAR,
                genre_listen_count BIGINT,
                genre_avg_duration DOUBLE PRECISION,
                genre_popularity_index DOUBLE PRECISION,
                top_track VARCHAR,
                top_artist VARCHAR
            );
            TRUNCATE public.kpi_staging;
        """,
        "postactions": """
            DELETE FROM public.kpi_table
            USING public.kpi_staging
            WHERE public.kpi_table.kpi_type = public.kpi_staging.kpi_type
              AND public.kpi_table.group_by = public.kpi_staging.group_by;

            INSERT INTO public.kpi_table
            SELECT * FROM public.kpi_staging;
        """
    },
    redshift_tmp_dir="s3://lab1-temp/redshift-tmp/"
)

job.commit()
