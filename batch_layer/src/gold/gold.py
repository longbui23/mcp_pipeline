"""
gold_processing.py

Functions for Gold layer processing using DuckDB:
- Business transformations
- Metrics & aggregations
- Referential integrity
- Expose for BI/ML
"""

import duckdb
import pandas as pd
from typing import Optional, Dict

# -----------------------------
# DuckDB connection helper
# -----------------------------
def get_connection(db_path: Optional[str] = "gold_layer.duckdb") -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection (persistent file or in-memory)
    """
    return duckdb.connect(db_path)


# -----------------------------
# 1) Business transformations
# -----------------------------
def transform_business_rules(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    target_table: str,
    rules_sql: str
):
    """
    Apply business transformation rules using SQL
    """
    query = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    SELECT *
    FROM {source_table}
    {rules_sql}
    """
    con.execute(query)
    print(f"Applied business rules and created {target_table}")


# -----------------------------
# 2) Metrics / Aggregations
# -----------------------------
def compute_metrics(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    metrics_table: str,
    group_by_cols: Optional[list] = None,
    metrics_sql: Optional[str] = None
):
    """
    Compute business metrics / aggregations.
    group_by_cols: list of columns to group by
    metrics_sql: additional SQL expressions for metrics
    """
    group_by = f"GROUP BY {', '.join(group_by_cols)}" if group_by_cols else ""
    metrics_select = metrics_sql if metrics_sql else "*"
    
    query = f"""
    CREATE OR REPLACE TABLE {metrics_table} AS
    SELECT {metrics_select}
    FROM {source_table}
    {group_by}
    """
    con.execute(query)
    print(f"Computed metrics and saved to {metrics_table}")


# -----------------------------
# 3) Referential integrity / validation
# -----------------------------
def validate_referential_integrity(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    dimension_table: str,
    fact_key: str,
    dim_key: str
) -> Dict[str, int]:
    """
    Validate referential integrity between fact and dimension tables
    """
    query = f"""
    SELECT COUNT(*) AS invalid_refs
    FROM {fact_table} f
    LEFT JOIN {dimension_table} d
    ON f.{fact_key} = d.{dim_key}
    WHERE d.{dim_key} IS NULL
    """
    result = con.execute(query).fetchone()
    print(f"Referential integrity check: {result[0]} invalid references")
    return {"invalid_references": result[0]}


# -----------------------------
# 4) Expose to BI / ML
# -----------------------------
def export_to_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: str
):
    """
    Export table to CSV for BI or ML consumption
    """
    con.execute(f"COPY {table_name} TO '{csv_path}' WITH (HEADER, DELIMITER ',');")
    print(f"Exported {table_name} to {csv_path}")


# -----------------------------
# Example Usage
# -----------------------------
if __name__ == "__main__":
    con = get_connection()

    # Example: create Gold fact table from Silver
    transform_business_rules(
        con,
        source_table="silver_data",
        target_table="gold_fact_sales",
        rules_sql="WHERE price > 100"
    )

    # Compute aggregated metrics
    compute_metrics(
        con,
        source_table="gold_fact_sales",
        metrics_table="gold_sales_metrics",
        group_by_cols=["name"],
        metrics_sql="name, COUNT(*) AS total_orders, SUM(price) AS total_sales"
    )

    # Validate referential integrity
    # Example: fact table has 'product_id', dimension table has 'id'
    validate_referential_integrity(
        con,
        fact_table="gold_fact_sales",
        dimension_table="gold_dim_products",
        fact_key="id",
        dim_key="id"
    )

    # Export for BI/ML
    export_to_csv(con, "gold_sales_metrics", "gold_sales_metrics.csv")
