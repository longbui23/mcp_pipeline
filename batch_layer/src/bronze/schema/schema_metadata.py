import os
import json
import pandas as pd
import psycopg2
from typing import Dict, Any, Optional

# -----------------------------
# Config
# -----------------------------
METADATA_DIR = "./metadata"
os.makedirs(METADATA_DIR, exist_ok=True)

# -----------------------------
# Helper Functions
# -----------------------------
def save_metadata(schema: Dict[str, Any], filename: str):
    """Save schema metadata to JSON"""
    path = os.path.join(METADATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"Saved metadata to {path}")


# -----------------------------
# CSV Schema Reader
# -----------------------------
def get_csv_schema(file_path: str) -> Dict[str, Any]:
    df = pd.read_csv(file_path, nrows=10)  # sample first 10 rows for types
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    return {
        "type": "csv",
        "path": file_path,
        "columns": schema,
        "rows_sampled": len(df)
    }


# -----------------------------
# JSON Schema Reader
# -----------------------------
def get_json_schema(file_path: str) -> Dict[str, Any]:
    df = pd.read_json(file_path, lines=True)  # assuming JSON lines
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    return {
        "type": "json",
        "path": file_path,
        "columns": schema,
        "rows_sampled": len(df)
    }


# -----------------------------
# Postgres Table Schema Reader
# -----------------------------
def get_postgres_schema(
    table_name: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str
) -> Dict[str, Any]:

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cur.fetchall()
    schema = {col[0]: {"type": col[1], "nullable": col[2]} for col in columns}
    cur.close()
    conn.close()
    return {
        "type": "postgres",
        "table": table_name,
        "columns": schema
    }


# -----------------------------
# Example Usage
# -----------------------------
if __name__ == "__main__":
    # Example CSV
    csv_file = "./data/sample.csv"
    csv_schema = get_csv_schema(csv_file)
    save_metadata(csv_schema, "sample_csv_schema.json")

    # Example JSON
    json_file = "./data/sample.json"
    json_schema = get_json_schema(json_file)
    save_metadata(json_schema, "sample_json_schema.json")

    # Example Postgres
    pg_schema = get_postgres_schema(
        table_name="users",
        host="localhost",
        port=5432,
        dbname="mydb",
        user="myuser",
        password="mypassword"
    )
    save_metadata(pg_schema, "users_pg_schema.json")
