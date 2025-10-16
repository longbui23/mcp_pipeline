from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Types
import json
import numpy as np
import tensorflow as tf

class LSTMRealtimePredictor(KeyedProcessFunction):
    def __init__(self, model_path: str, window_size: int = 20):
        self.model_path = model_path
        self.window_size = window_size
        # state descriptors
        self.seq_state = None  # will hold JSON list of floats
        self.model = None

    def open(self, runtime_context: RuntimeContext):
        # load Keras LSTM model once per parallel instance
        self.model = tf.keras.models.load_model(self.model_path)
        # create state: store the sliding window as JSON string
        self.seq_state = runtime_context.get_state("seq_window", Types.STRING())

    def _get_window(self):
        raw = self.seq_state.value()
        if raw is None:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def _set_window(self, window_list):
        self.seq_state.update(json.dumps(window_list))

    def process_element(self, value, ctx, out):
        """
        value: a JSON string or dict containing at least:
               { "device_id": "...", "timestamp": "...", "temperature": ... }
        out: Collector to emit prediction JSON strings
        """
        # normalize input to dict
        if isinstance(value, str):
            data = json.loads(value)
        else:
            data = value

        # extract feature(s). adjust if you have multivariate features.
        temp = float(data.get("temperature"))

        # load current window, append new value, keep last N
        window = self._get_window()
        window.append(temp)
        if len(window) > self.window_size:
            window = window[-self.window_size:]
        self._set_window(window)

        # If window not full, optionally emit warm-up info or skip prediction
        if len(window) < self.window_size:
            # emit partial info (optional)
            out.collect(json.dumps({
                "device_id": data.get("device_id"),
                "timestamp": data.get("timestamp"),
                "prediction_ready": False,
                "window_len": len(window)
            }))
            return

        # prepare input for LSTM: shape (1, window_size, features)
        # here features = 1 (temperature). For multiple features, change accordingly.
        arr = np.array(window, dtype=np.float32).reshape(1, self.window_size, 1)

        # run model prediction (synchronous)
        pred = self.model.predict(arr, verbose=0)  # returns shape (1, out_dim)
        pred_value = float(np.squeeze(pred))

        # emit prediction
        out.collect(json.dumps({
            "device_id": data.get("device_id"),
            "timestamp": data.get("timestamp"),
            "prediction": pred_value,
            "last_observed": temp
        }))

        # optional: do anomaly detection combining prediction and observed
        if abs(temp - pred_value) > 5.0:  # threshold example
            out.collect(json.dumps({
                "device_id": data.get("device_id"),
                "timestamp": data.get("timestamp"),
                "alert": "anomaly",
                "observed": temp,
                "predicted": pred_value
            }))
