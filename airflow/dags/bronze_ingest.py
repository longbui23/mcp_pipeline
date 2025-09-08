from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label

import pendulum

# Import your custom modules 
try:
    from src.bronze.api.yfinance import (
        fetch_price_for_symbol,
        fetch_recommendations,
        fetch_news,
        fetch_financials,
        fetch_symbols,
        fetch_all
    )
    from plugins.DuckDBOperator.conn import connect
except ImportError as e:
    print(f"Import error: {e}")
    raise


@dag(
    start_date=pendulum.datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md="dags/Bronze.md",
    default_args={
        "owner": "Long", 
        "retries": 3,
        "retry_exponential_backoff": True,  
        "email": ["blong5791@gmail.com"],  
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["bronze_ingest", "yfinance"],
)
def bronze_stock_ingest_test():
    # Start
    start = EmptyOperator(task_id="start")
    
    # Fetch Symbols
    fetch_symbols_task = PythonOperator(
        task_id="fetch_symbols",
        python_callable=fetch_symbols,
    )
    
    # Validate first symbol
    def validate_first_symbol(ti):
        try:
            symbols = ti.xcom_pull(task_ids="fetch_symbols")
            if not symbols:
                return False
            first_symbol = symbols[0]
            test_data = fetch_price_for_symbol(first_symbol, bucket="stories1512", s3_hook=None, period="5d", interval="1d")
            return bool(test_data)
        except Exception as e:
            print(f"Validation failed: {e}")
            return False
    
    validate_task = PythonOperator(
        task_id="validate_source",
        python_callable=validate_first_symbol,
    )

    # Branching operator
    def branch_on_source(ti):
        success = ti.xcom_pull(task_ids="validate_source")
        return "stop_dag" if not success else "fetch_all_data"
    
    branch_task = BranchPythonOperator(
        task_id="branch_on_source",
        python_callable=branch_on_source,
    )
    
    # Fetch ALL (prices, recs, news, financials for all symbols)
    fetch_all_task = PythonOperator(
        task_id="fetch_all_data",
        python_callable=lambda ti: fetch_all(
            symbols=ti.xcom_pull(task_ids="fetch_symbols"),
            bucket_name="stories1512",
            s3_hook=None  # replace with real S3 hook if needed
        ),
    )

    # End
    stop_dag = EmptyOperator(task_id="stop_dag")
    
    # -----------------------------
    # DAG dependencies
    # -----------------------------
    start >> fetch_symbols_task >> validate_task >> branch_task
    branch_task >> Label("Stop DAG") >> stop_dag
    branch_task >> Label("Continue") >> fetch_all_task >> stop_dag


# Instantiate DAG
bronze_stock_ingest_test()