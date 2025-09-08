# Bronze Ingest DAG - YFinance

This DAG is responsible for ingesting raw financial data from the YFinance API and storing it in the **Bronze layer** of our data lake. The Bronze layer contains raw, unprocessed data for further downstream processing (Silver & Gold layers).

## Overview

- **Source:** Yahoo Finance API via `src.bronze.api.yfinance`
- **Destination:** Bronze storage (Parquet/Delta on object storage or local folder)
- **Frequency:** Daily
- **Purpose:** Capture historical and real-time stock data, financials, news, and recommendations.

## Tasks

1. **Fetch Price Data**  
   Retrieves the latest stock prices for a list of symbols.

2. **Fetch Financials**  
   Downloads financial statements (balance sheet, income, cash flow) for symbols.

3. **Fetch News**  
   Collects the latest news articles related to tracked symbols.

4. **Fetch Recommendations**  
   Gets analyst recommendations for each symbol.

5. **Merge & Validate**  
   Combines data sources and performs basic validation to ensure data integrity.

6. **Save to Bronze**  
   Writes the ingested data to the Bronze layer with minimal transformation.

## Notes

- The Bronze layer **does not perform heavy cleaning or enrichment**. Its goal is to preserve raw source data for traceability and auditing.
- Data is partitioned by **date** and optionally by **symbol** for faster retrieval in downstream processing.
- Use this DAG as the first step in a **ETL/ELT pipeline** feeding Silver and Gold layers.

