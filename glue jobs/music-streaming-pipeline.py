import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Streaming data
Streamingdata_node1749737126240 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://lab1-rawdata/streams/"], "recurse": True}, transformation_ctx="Streamingdata_node1749737126240")

# Script generated for node Songs metadata
Songsmetadata_node1749737032478 = glueContext.create_dynamic_frame.from_catalog(database="mss_db", table_name="processed_songs_table", transformation_ctx="Songsmetadata_node1749737032478")

# Script generated for node Users metadata
Usersmetadata_node1749737087808 = glueContext.create_dynamic_frame.from_catalog(database="mss_db", table_name="processed_users_table", transformation_ctx="Usersmetadata_node1749737087808")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1749737359369 = ApplyMapping.apply(frame=Songsmetadata_node1749737032478, mappings=[("track_id", "string", "track_id", "string"), ("artists", "string", "artists", "string"), ("track_name", "string", "track_name", "string"), ("popularity", "long", "popularity", "long"), ("duration_ms", "long", "duration_ms", "long"), ("genre", "string", "genre", "string")], transformation_ctx="RenamedkeysforJoin_node1749737359369")

# Script generated for node Renamed keys for Join with Users
RenamedkeysforJoinwithUsers_node1749738188521 = ApplyMapping.apply(frame=Usersmetadata_node1749737087808, mappings=[("user_id", "long", "users_user_id", "long"), ("created_at", "timestamp", "users_created_at", "timestamp")], transformation_ctx="RenamedkeysforJoinwithUsers_node1749738188521")

# Script generated for node Renamed keys for Song + Streaming data
RenamedkeysforSongStreamingdata_node1749903267336 = ApplyMapping.apply(frame=RenamedkeysforJoin_node1749737359369, mappings=[("track_id", "string", "songs_track_id", "string"), ("artists", "string", "songs_artists", "string"), ("track_name", "string", "songs_track_name", "string"), ("popularity", "long", "songs_popularity", "long"), ("duration_ms", "long", "songs_duration_ms", "long"), ("genre", "string", "songs_genre", "string")], transformation_ctx="RenamedkeysforSongStreamingdata_node1749903267336")

# Script generated for node Song + Streaming data
Streamingdata_node1749737126240DF = Streamingdata_node1749737126240.toDF()
RenamedkeysforSongStreamingdata_node1749903267336DF = RenamedkeysforSongStreamingdata_node1749903267336.toDF()
SongStreamingdata_node1749737238213 = DynamicFrame.fromDF(Streamingdata_node1749737126240DF.join(RenamedkeysforSongStreamingdata_node1749903267336DF, (Streamingdata_node1749737126240DF['track_id'] == RenamedkeysforSongStreamingdata_node1749903267336DF['songs_track_id']), "left"), glueContext, "SongStreamingdata_node1749737238213")

# Script generated for node Join with Users
SongStreamingdata_node1749737238213DF = SongStreamingdata_node1749737238213.toDF()
RenamedkeysforJoinwithUsers_node1749738188521DF = RenamedkeysforJoinwithUsers_node1749738188521.toDF()
JoinwithUsers_node1749738094221 = DynamicFrame.fromDF(SongStreamingdata_node1749737238213DF.join(RenamedkeysforJoinwithUsers_node1749738188521DF, (SongStreamingdata_node1749737238213DF['user_id'] == RenamedkeysforJoinwithUsers_node1749738188521DF['users_user_id']), "left"), glueContext, "JoinwithUsers_node1749738094221")

# Script generated for node Enforce Schema
EnforceSchema_node1749738258997 = ApplyMapping.apply(frame=JoinwithUsers_node1749738094221, mappings=[("user_id", "string", "user_id", "bigint"), ("track_id", "string", "track_id", "string"), ("listen_time", "string", "listen_time", "timestamp"), ("songs_artists", "string", "artists", "string"), ("songs_track_name", "string", "track_name", "string"), ("songs_popularity", "long", "popularity", "long"), ("songs_duration_ms", "long", "duration_ms", "long"), ("songs_genre", "string", "genre", "string"), ("users_created_at", "timestamp", "created_at", "timestamp")], transformation_ctx="EnforceSchema_node1749738258997")

# Script generated for node music-streaming-data
EvaluateDataQuality().process_rows(frame=EnforceSchema_node1749738258997, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749737927786", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
musicstreamingdata_node1749738625456 = glueContext.getSink(path="s3://lab1-curateddata/mss-transformed/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="musicstreamingdata_node1749738625456")
musicstreamingdata_node1749738625456.setCatalogInfo(catalogDatabase="mss_db",catalogTableName="dim-music-streaming")
musicstreamingdata_node1749738625456.setFormat("glueparquet", compression="snappy")
musicstreamingdata_node1749738625456.writeFrame(EnforceSchema_node1749738258997)
job.commit()