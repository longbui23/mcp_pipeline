from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("bronze_streaming") \
    .getOrCreate()

# 1. Read from all topics using regex
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "topic_.*") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Convert value to string
bronze_stream = df.selectExpr(
    "CAST(topic AS STRING) as topic",
    "CAST(value AS STRING) as raw_value",
    "timestamp"
)

# 3. Write to bronze layer (Parquet)
query = bronze_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://my-bucket/bronze/") \
    .option("checkpointLocation", "s3a://my-bucket/checkpoints/bronze/") \
    .partitionBy("topic") \
    .start()

query.awaitTermination()