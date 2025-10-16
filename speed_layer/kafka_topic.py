from faker import Faker
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from config import DEVICES_TYPE

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'sensors'

def generate_random_id(prefix, id_range):
    start, end = int(id_range[0].split("_")[1]), int(id_range[1].split("_")[1])
    return f"{prefix}_{random.randint(start, end)}"

def generate_iot_data():
    device_type = random.choice(list(DEVICES_TYPE.keys()))
    device_config = DEVICES_TYPE[device_type]

    device_id = generate_random_id(device_type, device_config["device_id"])
    location_id = generate_random_id("loc", device_config["location_id"])

    data = {
        "device_id": device_id,
        "device_type": device_type,
        "location_id": location_id,
        "timestamp": datetime.now().isoformat()
    }

    for sensor, values in device_config.items():
        if sensor in ["device_id", "location_id"]:
            continue
        if isinstance(values, tuple):
            data[sensor] = round(random.uniform(*values), 2)
        elif isinstance(values, list):
            data[sensor] = random.choice(values)

    return data

while True:
    iot_data = generate_iot_data()
    producer.send(topic, value=iot_data)
    print(f"Sent: {iot_data}")
    time.sleep(1)