from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel

# Initialize Spark
spark = SparkSession.builder \
    .appName("RealTimeTemperaturePrediction") \
    .getOrCreate()

# Read streaming data from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "temperature_topic") \
    .load()

# Kafka value is bytes, convert to string
df = raw_df.selectExpr("CAST(value AS STRING)", "timestamp")

# Define a window for lag/rolling features
windowSpec = Window.orderBy("timestamp").rowsBetween(-4, 0)  # last 5 rows

# Rolling mean
df = df.withColumn("temp_roll_mean_5", avg("temperature").over(windowSpec))

# Lag feature (previous temperature)
df = df.withColumn("temp_lag_1", lag("temperature", 1).over(windowSpec))

feature_cols = ["temp_lag_1", "temp_roll_mean_5", "humidity"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df).select("features", "temperature")

# Assume you trained a LinearRegression model previously and saved it
model = LinearRegressionModel.load("models/temperature_model")

# Apply the model for real-time prediction
predictions = model.transform(df_features)

query = predictions.select("timestamp", "temperature", "prediction") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
