import os

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector import connect

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

CDC_API_TOKEN = os.getenv("CDC_API_TOKEN")
VACC_COVERAGE_URL = "https://data.cdc.gov/resource/ksfb-ug5d.json"

BATCH_SIZE = 30000

EXPECTED_COLUMNS = [
    "vaccine",
    "geographic_level",
    "geographic_name",
    "demographic_level",
    "demographic_name",
    "indicator_label",
    "indicator_category_label",
    "month_week",
    "week_ending",
    "estimate",
    "ci_half_width_95pct",
    "unweighted_sample_size",
    "current_season_week_ending",
    "covid_season",
    "suppression_flag",
]

def get_snowflake_connection():
    logger.info("Connecting to Snowflake...")

    conn = connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    return conn

def fetch_vaccination_coverage(offset):
    logger.info(f"Fetching vaccination coverage at offset {offset}")

    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$$app_token": CDC_API_TOKEN
    }

    response = requests.get(VACC_COVERAGE_URL, params=params)
    response.raise_for_status()

    return response.json()

def load_to_snowflake(coverage, conn):
    logger.info(f"Loading {len(coverage)} rows to Snowflake...")

    cursor = conn.cursor()

    for col in EXPECTED_COLUMNS:
        if col not in coverage.columns:
            coverage = coverage.with_columns(pl.lit(None).alias(col))

    coverage = coverage.select(EXPECTED_COLUMNS)

    rows = [
        tuple(None if value is None else value for value in row.values())
        for row in coverage.iter_rows(named=True)
    ]

    cursor.executemany(f"""
        INSERT INTO RAW.VACCINATION_COVERAGE VALUES (
            {', '.join(['%s'] * len(EXPECTED_COLUMNS))})
    """, rows)  # noqa: S608

    conn.commit()
    logger.info("Load Complete!")

def main():
    logger.info("Starting vaccination coverage ingestion...")

    conn = get_snowflake_connection()

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE RAW.VACCINATION_COVERAGE")
    logger.info("Table truncated, loading fresh data...")

    offset = 0
    total_rows = 0

    while True:
        data = fetch_vaccination_coverage(offset)

        if not data:
            logger.info("No more data to fetch. Ingestion complete!")
            break

        coverage = pl.DataFrame(data)
        load_to_snowflake(coverage, conn)

        total_rows += len(coverage)
        offset += BATCH_SIZE
        logger.info(f"Total rows loaded so far: {total_rows}")

    conn.close()
    logger.info(f"Ingestion complete! Total rows loaded: {total_rows}")

if __name__ == "__main__":
    main()

