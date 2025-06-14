import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node songs metadata
songsmetadata_node1749729746902 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://lab1-rawdata/metadata/songs.csv"], "recurse": True}, transformation_ctx="songsmetadata_node1749729746902")

# Script generated for node users metadata
usersmetadata_node1749730163416 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://lab1-rawdata/metadata/users.csv"], "recurse": True}, transformation_ctx="usersmetadata_node1749730163416")

# Script generated for node songs enforce schema
songsenforceschema_node1749730333693 = ApplyMapping.apply(frame=songsmetadata_node1749729746902, mappings=[("track_id", "string", "track_id", "string"), ("artists", "string", "artists", "string"), ("track_name", "string", "track_name", "string"), ("popularity", "string", "popularity", "long"), ("duration_ms", "string", "duration_ms", "long"), ("track_genre", "string", "genre", "string")], transformation_ctx="songsenforceschema_node1749730333693")

# Script generated for node users enforce schema
usersenforceschema_node1749730287107 = ApplyMapping.apply(frame=usersmetadata_node1749730163416, mappings=[("user_id", "string", "user_id", "long"), ("created_at", "string", "created_at", "timestamp")], transformation_ctx="usersenforceschema_node1749730287107")

# Script generated for node songs s3
EvaluateDataQuality().process_rows(frame=songsenforceschema_node1749730333693, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749727544130", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
songss3_node1749730675410 = glueContext.getSink(path="s3://lab1-curateddata/processed-metadata/songs/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="songss3_node1749730675410")
songss3_node1749730675410.setCatalogInfo(catalogDatabase="mss_db",catalogTableName="processed_songs_table")
songss3_node1749730675410.setFormat("glueparquet", compression="snappy")
songss3_node1749730675410.writeFrame(songsenforceschema_node1749730333693)
# Script generated for node users s3
EvaluateDataQuality().process_rows(frame=usersenforceschema_node1749730287107, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749727544130", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
userss3_node1749730739409 = glueContext.getSink(path="s3://lab1-curateddata/processed-metadata/users/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="userss3_node1749730739409")
userss3_node1749730739409.setCatalogInfo(catalogDatabase="mss_db",catalogTableName="processed_users_table")
userss3_node1749730739409.setFormat("glueparquet", compression="snappy")
userss3_node1749730739409.writeFrame(usersenforceschema_node1749730287107)
job.commit()