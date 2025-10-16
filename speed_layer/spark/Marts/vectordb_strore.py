# streaming_silver_to_influx_env.py

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from influxdb_client import InfluxDBClient, Point, WritePrecision

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
SILVER_PATH = os.getenv("SILVER_PATH")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH")

# -----------------------------
# Persistent InfluxDB connection
# -----------------------------
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

# -----------------------------
# Spark session
# -----------------------------
spark = SparkSession.builder.appName("SilverToInfluxRealtime").getOrCreate()

# -----------------------------
# Read silver layer (streaming)
# -----------------------------
df = spark.readStream.format("parquet").load(SILVER_PATH)
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# -----------------------------
# Write directly to InfluxDB
# -----------------------------
query = df.writeStream.foreach(
    lambda row: write_api.write(
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
        record=Point("sensor_data")
            .tag("device_id", str(row["id"]))
            .field("voltage", float(row.get("voltage", 0.0)))
            .field("current", float(row.get("current", 0.0)))
            .field("power", float(row.get("power", 0.0)))
            .field("temperature", float(row.get("temperature", 0.0)))
            .field("humidity", float(row.get("humidity", 0.0)))
            .time(row["timestamp"], WritePrecision.S)
    )
).outputMode("append") \
.option("checkpointLocation", CHECKPOINT_PATH) \
.start()

query.awaitTermination()
