"""
gold_dag.py

Airflow DAG for Gold layer processing using DuckDBOperator.
"""

from airflow.decorators import dag
from pendulum import datetime
from plugins.DuckDBOperator.DuckDBOperator import DuckDBOperator 

# -----------------------------
# DAG parameters
# -----------------------------
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold_layer"],
    doc_md=__doc__,
)
def gold_layer_dag():

    # -----------------------------
    # Business transformation
    # -----------------------------
    business_transform = DuckDBOperator(
        task_id="business_transform",
        duckdb_conn_id="duckdb_default",
        sql="""
            CREATE OR REPLACE TABLE gold_fact_sales AS
            SELECT *
            FROM silver_data
            WHERE price > 100;
        """
    )

    # -----------------------------
    # Compute aggregated metrics
    # -----------------------------
    compute_metrics = DuckDBOperator(
        task_id="compute_metrics",
        duckdb_conn_id="duckdb_default",
        sql="""
            CREATE OR REPLACE TABLE gold_sales_metrics AS
            SELECT 
                name,
                COUNT(*) AS total_orders,
                SUM(price) AS total_sales
            FROM gold_fact_sales
            GROUP BY name;
        """
    )

    # -----------------------------
    # Referential integrity validation
    # -----------------------------
    validate_integrity = DuckDBOperator(
        task_id="validate_integrity",
        duckdb_conn_id="duckdb_default",
        sql="""
            SELECT COUNT(*) AS invalid_records
            FROM gold_fact_sales f
            LEFT JOIN gold_dim_products d
            ON f.id = d.id
            WHERE d.id IS NULL;
        """
    )

    # -----------------------------
    # Export for BI/ML
    # -----------------------------
    export_bi_ml = DuckDBOperator(
        task_id="export_bi_ml",
        duckdb_conn_id="duckdb_default",
        sql="""
            COPY gold_sales_metrics TO 'gold_sales_metrics.csv' (HEADER, DELIMITER ',');
        """
    )

    # -----------------------------
    # Task dependencies
    # -----------------------------
    business_transform >> compute_metrics >> validate_integrity >> export_bi_ml


# Instantiate DAG
gold_layer_dag()