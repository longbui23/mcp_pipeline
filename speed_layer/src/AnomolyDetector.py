from pyflink.datastream.functions import KeyedProcessFunction
import json

class AnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.temp_state = None

    def open(self, ctx):
        self.temp_state = ctx.get_state("avg_temp", Types.FLOAT())

    def process_element(self, value, ctx):
        data = json.loads(value)
        prev_avg = self.temp_state.value() or 0
        new_avg = (prev_avg * 0.9) + (data["temperature"] * 0.1)
        self.temp_state.update(new_avg)
        if abs(data["temperature"] - new_avg) > 5:
            print(f"🚨 Anomaly detected for {data['device_id']} → {data['temperature']}")`