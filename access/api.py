from fastapi import FastAPI
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "iot_bucket"

app = FastAPI(title="IoT API", version="1.0")

# Connect to InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api: QueryApi = client.query_api()

@app.get("/")
def root():
    return {"message": "IoT API running"}

@app.get("/devices")
def list_devices():
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -1h)
        |> group(columns: ["device_id"])
        |> distinct(column: "device_id")
    '''
    tables = query_api.query(query)
    devices = [record.get_value() for table in tables for record in table.records]
    return {"devices": devices}

@app.get("/device/{device_id}")
def get_device_data(device_id: str):
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -10m)
        |> filter(fn: (r) => r.device_id == "{device_id}")
        |> last()
    '''
    tables = query_api.query(query)
    data = [record.values for table in tables for record in table.records]
    return {"device_id": device_id, "data": data}

@app.get("/metrics/temperature")
def get_temperature_metrics():
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -5m)
        |> filter(fn: (r) => r._measurement == "iot_measurements" and r._field == "temperature")
    '''
    tables = query_api.query(query)
    results = [{"time": record["_time"], "value": record["_value"], "device": record["device_id"]} 
               for table in tables for record in table.records]
    return results
