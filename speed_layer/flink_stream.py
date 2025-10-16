from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime

# InfluxDB config
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "iot_bucket"

# Kafka topics (one per device type)
KAFKA_TOPICS = [
    "thermostat_topic",
    "air_quality_sensor_topic",
    "smart_light_topic",
    "motion_sensor_topic",
    "water_meter_topic"
]

# Function to write to InfluxDB dynamically
def write_to_influx(record):
    data = json.loads(record)
    device_id = data.get("device_id")
    device_type = data.get("device_type")
    timestamp = datetime.fromisoformat(data.get("timestamp"))

    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
        write_api = client.write_api()
        point = Point("iot_measurements").tag("device_id", device_id).tag("device_type", device_type).time(timestamp, WritePrecision.NS)

        # Dynamically add all numeric / string fields except device_id, device_type, location_id, timestamp
        for key, value in data.items():
            if key in ["device_id", "device_type", "location_id", "timestamp"]:
                continue
            if isinstance(value, (int, float)):
                point.field(key, float(value))
            elif isinstance(value, str):
                point.field(key, value)
            elif isinstance(value, bool):
                point.field(key, int(value)) 

        # Optionally add location_id as a tag
        if "location_id" in data:
            point.tag("location_id", data["location_id"])

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

# Initialize Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Kafka consumer (multi-topic)
kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPICS,
    deserialization_schema=SimpleStringSchema(),
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink_iot_group'}
)

# Add source
stream = env.add_source(kafka_consumer)

# Map JSON -> InfluxDB write
stream.map(write_to_influx, output_type=Types.VOID())

# Execute Flink job
env.execute("IoT Kafka Multi-Topic to InfluxDB Stream")