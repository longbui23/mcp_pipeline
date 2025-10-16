import yfinance as yf
import pandas as pd
import json
import pendulum
import boto3
import os

today = pendulum.now().to_date_string()


def update_health(health_report: dict, df: pd.DataFrame, symbol: str, dataset: str):
    """Aggregate health stats into the shared health report"""
    if df is None or df.empty:
        return

    if symbol not in health_report:
        health_report[symbol] = {}

    stats = health_report[symbol].get(dataset, {
        "row_count": 0,
        "null_counts": {},
        "duplicate_rows": 0,
    })

    # Row count
    stats["row_count"] += len(df)

    # Null counts
    nulls = df.isnull().sum().to_dict()
    for col, val in nulls.items():
        stats["null_counts"][col] = stats["null_counts"].get(col, 0) + int(val)

    # Duplicates
    stats["duplicate_rows"] += int(df.duplicated().sum())

    health_report[symbol][dataset] = stats


def save_schema(df: pd.DataFrame, symbol: str, dataset: str, bucket: str, s3_hook):
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    key = f"bronze/{symbol}/_meta/schema_{dataset}_{today}.json"
    s3_hook.put_object(Body=json.dumps(schema, indent=2), Bucket=bucket, Key=key)


# ------------------------
# Fetch price
# ------------------------
def fetch_price_for_symbol(symbol: str, bucket: str, s3_hook, health_report, period="1d", interval="1d"):
    df = yf.Ticker(symbol).history(period=period, interval=interval)
    if df is None or df.empty:
        return

    df.reset_index(inplace=True)
    df.columns = df.columns.map(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else x)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    key = f"bronze/{symbol}/price/price_{today}.json"
    s3_hook.put_object(Body=json.dumps(records, default=str), Bucket=bucket, Key=key)

    update_health(health_report, df, symbol, "price")
    save_schema(df, symbol, "price", bucket, s3_hook)


# ------------------------
# Fetch recommendations
# ------------------------
def fetch_recommendations(symbol: str, bucket: str, s3_hook, health_report):
    df = yf.Ticker(symbol).recommendations
    if df is None or df.empty:
        return

    df.reset_index(inplace=True)
    df.columns = df.columns.map(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else x)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    key = f"bronze/{symbol}/recommendations/recs_{today}.json"
    s3_hook.put_object(Body=json.dumps(records, default=str), Bucket=bucket, Key=key)

    update_health(health_report, df, symbol, "recommendations")
    save_schema(df, symbol, "recommendations", bucket, s3_hook)


# ------------------------
# Fetch news
# ------------------------
def fetch_news(symbol: str, bucket: str, s3_hook, health_report):
    news = yf.Ticker(symbol).news
    if not news:
        return

    df = pd.json_normalize(news)
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, (int, float)) and len(str(int(x))) >= 10).all():
            # Convert UNIX timestamp to date
            df[col] = pd.to_datetime(df[col], unit='s', errors='ignore')

    # Fill nulls
    df = df.where(pd.notnull(df), None)

    update_health(health_report, df, symbol, "news")


    key = f"bronze/{symbol}/news/news_{today}.json"
    s3_hook.put_object(Body=json.dumps(news, default=str), Bucket=bucket, Key=key)

    update_health(health_report, df, symbol, "news")
    save_schema(df, symbol, "news", bucket, s3_hook)


# ------------------------
# Fetch financials
# ------------------------
def fetch_financials(symbol: str, bucket: str, s3_hook, health_report):
    df = yf.Ticker(symbol).financials
    if df is None or df.empty:
        return

    df.reset_index(inplace=True)
    df.columns = df.columns.map(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else x)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    key = f"bronze/{symbol}/financials/financials_{today}.json"
    s3_hook.put_object(Body=json.dumps(records, default=str), Bucket=bucket, Key=key)

    update_health(health_report, df, symbol, "financials")
    save_schema(df, symbol, "financials", bucket, s3_hook)


# ------------------------
# Fetch all
# ------------------------
# ------------------------
# Fetch all with backfill
# ------------------------
def fetch_all(bucket_name, s3_hook, start_date=None, end_date=None):
    """Fetch data for all symbols."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-2"
    )

    obj = s3.get_object(Bucket="stories1512", Key="bronze/sp500.csv")
    symbols = pd.read_csv(obj["Body"])["Symbol"].tolist()
    health_report = {"generated_at": today, "runs": []}

    if start_date and end_date:
        dates = pendulum.period(pendulum.parse(start_date), pendulum.parse(end_date)).range("days")
    else:
        dates = [pendulum.today()]

    for run_date in dates:
        run_date_str = run_date.to_date_string()
        run_health = {"date": run_date_str, "symbols": {}}

        for symbol in symbols:
            fetch_price_for_symbol(symbol, bucket_name, s3_hook, run_health["symbols"], period="1d", interval="1d")
            fetch_recommendations(symbol, bucket_name, s3_hook, run_health["symbols"])
            fetch_news(symbol, bucket_name, s3_hook, run_health["symbols"])
            fetch_financials(symbol, bucket_name, s3_hook, run_health["symbols"])

        health_report["runs"].append(run_health)

    # Save aggregated health report (covering all runs)
    key = f"bronze/_meta/health_{pendulum.now().to_date_string()}.json"
    s3_hook.put_object(
        Body=json.dumps(health_report, indent=2),
        Bucket=bucket_name,
        Key=key,
    )

    report_key = f"HealthReport/{pendulum.now().to_date_string()}.json"
    s3_hook.load_string(
        string_data=json.dumps(health_report, indent=2),
        key=report_key,
        bucket_name=bucket_name,
        replace=True,
    )

    return 200
