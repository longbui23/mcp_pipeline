from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, TimestampType,
)

# Sensor
SENSOR_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("current", DoubleType(), True),
    StructField("power", DoubleType(), True),
    StructField("accum", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("flow_rate", DoubleType(), True),
    StructField("energy_factor", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("signal_strength", IntegerType(), True)
])
