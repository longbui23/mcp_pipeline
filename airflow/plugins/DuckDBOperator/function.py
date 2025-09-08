import duckdb
import json
import types

# Recursive dict flattening
def _flatten_dict(d: dict, prefix=""):
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out.update(_flatten_dict(v, prefix + k + "."))
        else:
            out[prefix + k] = v
    return out

# Function to read & flatten JSON file and return a relation
def _flatten_json_conn(self, json_file_path: str):
    # Load JSON file
    with open(json_file_path, "r") as f:
        data = json.load(f)
    
    if isinstance(data, dict):
        data = [data]  # single object → list
    
    # Flatten each object
    flattened_data = [_flatten_dict(item) for item in data]
    
    # Register a temporary view
    self.register("tmp_flattened_json", flattened_data)
    
    # Return a DuckDB relation
    return self.execute("SELECT * FROM tmp_flattened_json")