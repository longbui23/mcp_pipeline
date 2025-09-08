"""
Functions:

1. get_connection

2. load_json
   - Loads a JSON file into DuckDB as a view.
   - Validates that the source is available.
   - Supports JSON Lines and array JSON.

3. transform_silver
   - Deduplicates rows.
   - Handles nulls and imputes default values.
   - Casts columns to specified data types.
   - Applies schema enforcement.
   - Validates that transformation produces non-empty results.

4. save_parquet
   - Saves the Silver-transformed data to Parquet format
   - Overwrites existing files if necessary.
"""


#import pandas as pd
#from typing import Dict, Any, List, Tuple, Optional
import duckdb
import os

# ----------------------------
# 1. Connect to DuckDB
# ----------------------------
def get_connection(database=":memory:"):
    return duckdb.connect(database=database)


# ----------------------------
# 2. Load JSON as DuckDB view
# ----------------------------
def load_json(con, json_file: str, view_name="bronze_source", json_format="auto"):
    if not json_file:
        return 
    con.execute(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM read_json_auto('{json_file}', format='{json_format}');
    """)
    tables = con.execute("SHOW TABLES").df()["name"].tolist()
    assert view_name in tables, f"❌ Source JSON '{json_file}' not loaded!"
    print(f"✅ Loaded JSON '{json_file}' into view '{view_name}'")
    return view_name


# ----------------------------
# 3. Silver Transformation
# ----------------------------
def transform_silver(con, source_view: str, dedup=True, null_rules=None, cast_types=None):
    if null_rules is None:
        null_rules = {}
    if cast_types is None:
        cast_types = {}

    coalesce_exprs = []
    for col in con.execute(f"PRAGMA table_info('{source_view}')").df()['name']:
        default = null_rules.get(col, 'NULL')
        coalesce_exprs.append(f"COALESCE({col}, {repr(default)}) AS {col}")

    cast_exprs = []
    for col in con.execute(f"PRAGMA table_info('{source_view}')").df()['name']:
        dtype = cast_types.get(col, None)
        cast_exprs.append(f"CAST({col} AS {dtype}) AS {col}" if dtype else col)

    dedup_sql = "DISTINCT" if dedup else ""

    silver_query = f"""
    WITH cleaned AS (
        SELECT {dedup_sql} * FROM {source_view}
    ),
    imputed AS (
        SELECT {', '.join(coalesce_exprs)} FROM cleaned
    ),
    casted AS (
        SELECT {', '.join(cast_exprs)} FROM imputed
    )
    SELECT * FROM casted;
    """

    silver_df = con.execute(silver_query).df()
    assert not silver_df.empty, "❌ Silver transformation produced empty result!"
    print("✅ Silver transformation successful")
    return silver_query, silver_df


# ----------------------------
# 4. Save to DuckDB Table or Parquet
# ----------------------------
def save_silver(con, silver_query: str, table_name: str = None, parquet_path: str = None):
    """
    Saves the Silver transformation either to:
      - DuckDB table (create if missing, else insert into)
      - Parquet file (if parquet_path is provided)
    """
    # Save to DuckDB table
    if table_name:
        tables = con.execute("SHOW TABLES").df()["name"].tolist()
        if table_name not in tables:
            # Create new table
            con.execute(f"CREATE TABLE {table_name} AS {silver_query}")
            print(f"✅ Table '{table_name}' created in DuckDB")
        else:
            # Insert into existing table
            con.execute(f"INSERT INTO {table_name} {silver_query}")
            print(f"✅ Data inserted into existing table '{table_name}'")

    # Save to Parquet
    if parquet_path:
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
        con.execute(f"COPY ({silver_query}) TO '{parquet_path}' (FORMAT PARQUET)")
        print(f"✅ Silver data written to Parquet '{parquet_path}'")