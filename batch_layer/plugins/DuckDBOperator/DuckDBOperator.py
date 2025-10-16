from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
import duckdb

class DuckDBOperator(BaseOperator):
    def __init__(self, sql: str, duckdb_conn_id: str = "duckdb_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id

    def execute(self, context):
        # Get connection from Airflow
        conn = BaseHook.get_connection(self.duckdb_conn_id)
        db_path = conn.host or ':memory:'  # Use in-memory if no path is provided

        # Connect to DuckDB
        con = duckdb.connect(database=db_path)
        try:
            result = con.execute(self.sql).fetchall()
            self.log.info(f"Query result: {result}")
            return result
        finally:
            con.close()