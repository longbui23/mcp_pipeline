# packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, when, lit
from delta.tables import DeltaTable

# init session
spark = SparkSession.builder \
    .appName('transforming_upsert') \
    .getOrCreate()

# read streaming data from Bronze
df = spark.readStream \
    .format('parquet') \
    .load('s3://my-bucket/bronze/')

# handle duplicates
df = df.withWatermark("timestamp", "2 minutes") \
       .dropDuplicates(['id', 'timestamp'])

# filter only OK status
df = df.filter(col('status') == "OK")

# conditional conversions per device
df = df.withColumn(
    "temperature",
    when(col('device') == "Sterilizer109", (col("temperature") - 32) * 5/9)
    .otherwise(col("temperature"))
).withColumn(
    "length",
    when(col('device') == "destroyer", col("length") * 3.28084)
    .otherwise(col("length"))
)

# cleaning and imputation
df = df \
    .withColumn("voltage", when(col("voltage").isNull(), lit(0.0)).otherwise(col("voltage"))) \
    .withColumn("current", when(col("current").isNull(), lit(0.0)).otherwise(col("current"))) \
    .withColumn("power", when(col("power").isNull(), col("voltage") * col("current")).otherwise(col("power"))) \
    .withColumn("temperature", when(col("temperature").isNull(), lit(0.0)).otherwise(col("temperature"))) \
    .withColumn("humidity", when(col("humidity").isNull(), lit(0.0)).otherwise(col("humidity"))) \
    .withColumn("vibration", when(col("vibration").isNull(), lit(0.0)).otherwise(col("vibration"))) \
    .withColumn("status", when(col("status").isNull(), lit("Error")).otherwise(col("status")))

# aggregating over time windows
df_agg = df.groupBy(
    "id",
    window("timestamp", "1 minute", "30 seconds")
).agg(
    avg("temperature").alias("avg_temp"),
    avg("humidity").alias("avg_humidity"),
    avg("power").alias("avg_power"),
    avg("voltage").alias("avg_voltage"),
    avg("current").alias("avg_current")
)

# select final columns
df_final = df_agg.select(
    col("id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "avg_temp",
    "avg_humidity",
    "avg_power",
    "avg_voltage",
    "avg_current"
)

# Delta Silver table path
silver_path = "s3a://my-bucket/silver/"
checkpoint_path = "s3a://my-bucket/checkpoints/silver/"

# define upsert function for foreachBatch
def upsert_to_silver(batch_df, batch_id):

    # create delta table if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, silver_path):
        batch_df.write.format("delta").mode("overwrite").save(silver_path)
        return

    delta_table = DeltaTable.forPath(spark, silver_path)

    # merge new batch into Silver
    delta_table.alias("silver").merge(
        batch_df.alias("bronze"),
        """
        silver.id = bronze.id AND 
        silver.window_start = bronze.window_start AND 
        silver.window_end = bronze.window_end
        """
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

# write stream with foreachBatch for Delta upsert
query = df_final.writeStream \
    .foreachBatch(upsert_to_silver) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()