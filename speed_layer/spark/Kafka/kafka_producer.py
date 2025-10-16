from kafka import KafkaProducer
import json, random, time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_topic_data():
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    data = {
        "topic_drill_string": {
            "id": f"drill_{random.randint(1,5):03d}",
            "timestamp": now,
            "vibration": round(random.uniform(0,0.05), 3),
            "torque": round(random.uniform(1000,1500),1),
            "rpm": random.randint(1400,1500),
            "drag": round(random.uniform(4,6),2),
            "hookload": round(random.uniform(250,350),1)
        },
        "topic_mud_logging": {
            "id": f"mud_{random.randint(1,5):03d}",
            "timestamp": now,
            "mud_density": round(random.uniform(10,12),2),
            "flow_rate": round(random.uniform(100,130),1),
            "viscosity": round(random.uniform(30,40),1),
            "weight": round(random.uniform(1400,1600),1)
        },
        "topic_bit_data": {
            "id": f"bit_{random.randint(1,5):03d}",
            "timestamp": now,
            "bit_pressure": round(random.uniform(2000,3000),1),
            "bit_temperature": round(random.uniform(80,90),1),
            "wear": round(random.uniform(0,0.02),3),
            "penetration_rate": round(random.uniform(1,2),2)
        },
        "topic_well_positioning": {
            "id": f"well_{random.randint(1,5):03d}",
            "timestamp": now,
            "depth": round(random.uniform(2000,3000),1),
            "inclination": round(random.uniform(0,10),2),
            "azimuth": round(random.uniform(0,360),1),
            "deviation": round(random.uniform(0,5),2)
        },
        "topic_hydraulics": {
            "id": f"pump_{random.randint(1,5):03d}",
            "timestamp": now,
            "pump_pressure": round(random.uniform(3000,4000),1),
            "pump_rate": round(random.uniform(100,150),1),
            "fluid_volume": round(random.uniform(4000,6000),1)
        },
        "topic_casing_cementing": {
            "id": f"cement_{random.randint(1,5):03d}",
            "timestamp": now,
            "cement_density": round(random.uniform(14,16),2),
            "slurry_flow": round(random.uniform(200,300),1),
            "pump_pressure": round(random.uniform(3000,3500),1)
        },
        "topic_formation_eval": {
            "id": f"formation_{random.randint(1,5):03d}",
            "timestamp": now,
            "gamma_ray": round(random.uniform(100,150),1),
            "resistivity": round(random.uniform(10,20),1),
            "porosity": round(random.uniform(0.2,0.3),2),
            "density": round(random.uniform(2.3,2.5),2)
        },
        "topic_surface_equipment": {
            "id": f"surface_{random.randint(1,5):03d}",
            "timestamp": now,
            "hoist_status": random.choice(["ON","OFF"]),
            "drawworks_rpm": random.randint(40,60),
            "pump_motor_status": random.choice(["OK","FAIL"]),
            "mud_motor_rpm": random.randint(1400,1500)
        },
        "topic_safety_env": {
            "id": f"env_{random.randint(1,5):03d}",
            "timestamp": now,
            "gas_level": round(random.uniform(5,20),1),
            "temperature": round(random.uniform(30,40),1),
            "humidity": round(random.uniform(40,50),1),
            "alarm": random.choice([True, False])
        },
        "topic_filling_storage": {
            "id": f"tank_{random.randint(1,5):03d}",
            "timestamp": now,
            "mud_volume": round(random.uniform(4000,6000),1),
            "cuttings_volume": round(random.uniform(200,300),1),
            "solids_content": round(random.uniform(0.1,0.2),2)
        }
    }
    return data


# Continuous streaming loop
while True:
    all_data = generate_topic_data()
    for topic, msg in all_data.items():
        producer.send(topic, msg)
    producer.flush()  
    time.sleep(5)  


