import pandas as pd

def infer_schema_from_df(df: pd.DataFrame):
    schema = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            schema[col] = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            schema[col] = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            schema[col] = "TIMESTAMP"
        else:
            schema[col] = "STRING"
    return schema
