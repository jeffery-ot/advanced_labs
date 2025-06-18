from pyspark.sql import SparkSession

# IAM Role ARN
iam_role = "arn:aws:iam::648637468459:role/lab1-gluerole"

# Correct JDBC URL with IAM-based access
jdbc_url = (
    "jdbc:redshift:iam://default-workgroup.648637468459.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
    "?DbUser=admin"
    "&ClusterIdentifier=default-workgroup"
    "&Region=us-east-1"
    f"&IamRole={iam_role}"
)

# Start Spark session
spark = SparkSession.builder \
    .appName("RedshiftIAMTest") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

try:
    # Test query to validate connection
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", "SELECT 1 AS test_col") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .load()

    df.show()
    print("✅ SUCCESS: IAM-based Redshift connection works!")

except Exception as e:
    print("❌ FAILED: Could not connect to Redshift via IAM")
    print(e)

# Stop Spark session
spark.stop()
