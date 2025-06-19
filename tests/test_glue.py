import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a test DataFrame
data = [("Jeffery", 28), ("Kwame", 34)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Show DataFrame content
df.printSchema()
df.show()

print("Glue container is working 🎉")
