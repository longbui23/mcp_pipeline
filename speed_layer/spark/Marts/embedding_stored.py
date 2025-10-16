from pyspark.sql import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, FloatType
import openai
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType

spark = SparkContext.builder \
        .appName("Embedding") \
        .getOrCreate()

silver_df = spark.read.parquet("s3://my-bucket/silver")

# convert rows to text
silver_texts = silver_df.select_Exp(
    "id", "window_start", "window_end",
    "concat('Sensor ', id, ': avg_temp=', avg_temp, ', avg_power=', avg_power) as text"
)

# text embedding
def embedded_text(text):
    emb = openai.Embedding.create(
        input=text,
        model='text-embedding-small',
    )['data'][0]['embedding']
    
    return [float(x) for x in emb]


embedding_udf = udf(silver_texts, ArrayType(FloatType()))
spark_df_with_emb = silver_df.withColumn("embedding", embedding_udf(col("text")))

# insert into vectordb
connections.connect(
    "default", 
    host="localhost", 
    port="19530"
)

fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
    FieldSchema(name="timestamp", dtype=DataType.INT64)
]
schema = CollectionSchema(fields)
collection = Collection("sensor_embeddings", schema)


records = []
for row in spark_df_with_emb.toLocalIterator():
    records.append([int(row['id']), row['embedding'], int(row['timestamp'].timestamp())])
collection.insert(records)
