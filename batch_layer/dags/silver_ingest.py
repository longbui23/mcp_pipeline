from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
import pendulum
import os, json, logging

from plugins.DuckDBOperator.DuckDBOperator import DuckDBOperator
duckdb_conn_id = "defaults"

# Paths and table names
JSON_SOURCE = "/path/to/bronze_data.json"
SILVER_TABLE = "silver_layer"
SILVER_PARQUET = "silver_data.parquet"

NULL_FILL = {"id": -1, "price": 0, "name": "UNKNOWN", "category": "UNKNOWN"}
CATEGORICAL_MAP = {"category": {"A": "Type1", "B": "Type2", "C": "Type3"}}  # example mapping

@dag(
    start_date=pendulum.datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["silver_layer", "duckdb"],
)
def silver_layer_ingest():
    start = EmptyOperator(task_id="start")
    stop_dag = EmptyOperator(task_id="stop_dag")

    def validate_source():
        if not os.path.exists(JSON_SOURCE):
            logging.warning(f"{JSON_SOURCE} not found")
            return False
        with open(JSON_SOURCE) as f:
            data = json.load(f)
        return bool(data)

    branch_task = BranchPythonOperator(
        task_id="branch_on_source",
        python_callable=lambda: "stop_dag" if not validate_source() else "load_json_task"
    )

    # 1. Load Bronze JSON → temporary in-memory table
    load_task = DuckDBOperator(
        task_id="load_json_task",
        sql=f"""
            CREATE OR REPLACE TEMP TABLE bronze_json AS
            SELECT * FROM read_json_auto('{JSON_SOURCE}');
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # 2. Load existing Silver Parquet → temporary table
    load_parquet_task = DuckDBOperator(
        task_id="load_parquet_task",
        sql=f"""
            CREATE OR REPLACE TEMP TABLE silver_parquet AS
            SELECT * FROM read_parquet('{SILVER_PARQUET}');
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # 3. Combine JSON + Parquet in-memory
    combine_task = DuckDBOperator(
        task_id="combine_task",
        sql=f"""
            CREATE OR REPLACE TEMP TABLE combined AS
            SELECT * FROM bronze_json
            UNION ALL
            SELECT * FROM silver_parquet;
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # 4. Transform / calculate indicators → Silver
    transform_task = DuckDBOperator(
        task_id="transform_silver_task",
        sql=f"""
            CREATE OR REPLACE TABLE {SILVER_TABLE} AS
            WITH ma_bb AS (
                SELECT
                    Date,
                    Close,
                    AVG(Close) OVER(ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS MA20,
                    AVG(Close) OVER(ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS MA50,
                    AVG(Close) OVER(ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS MA200,
                    AVG(Close) OVER(ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS BB_Mid,
                    STDDEV(Close) OVER(ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS BB_Std
                FROM combined
            )
            SELECT
                Date,
                Close,
                MA20,
                MA50,
                MA200,
                BB_Mid,
                BB_Std,
                BB_Mid + 2*BB_Std AS BB_Upper,
                BB_Mid - 2*BB_Std AS BB_Lower
            FROM ma_bb
            ORDER BY Date;
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # 5. Validate Silver data
    validate_task = DuckDBOperator(
        task_id="validate_silver_task",
        sql=f"""
            SELECT CASE WHEN COUNT(*) = 0 THEN 'VALID' ELSE 'INVALID' END AS validation_status
            FROM {SILVER_TABLE}
            WHERE Close IS NULL;
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # 6. Save Silver to Parquet
    save_task = DuckDBOperator(
        task_id="save_silver_task",
        sql=f"""
            COPY {SILVER_TABLE} TO '{SILVER_PARQUET}' (FORMAT PARQUET);
        """,
        duckdb_conn_id=duckdb_conn_id
    )

    # DAG flow
    start >> branch_task
    branch_task >> Label("Stop DAG") >> stop_dag
    branch_task >> Label("Continue") >> load_task >> load_parquet_task >> combine_task
    combine_task >> transform_task >> validate_task >> save_task

silver_layer_ingest()