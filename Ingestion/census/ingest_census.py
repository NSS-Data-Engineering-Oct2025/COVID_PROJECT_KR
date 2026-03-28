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

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
CENSUS_BASE_URL = "https://api.census.gov/data/2021/acs/acs5"

EXPECTED_COLUMNS = [
    "name",
    "b01003_001e",
    "b19013_001e",
    "b01002_001e",
    "b27001_001e",
    "b17001_002e",
    "state",
]

def get_snowflake_connection():
    logger.info("Connecting to Snowflake...")
    conn = connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )
    return conn

def fetch_census_data():
    logger.info("Fetching Census data...")

    params = {
        "get": "NAME,B01003_001E,B19013_001E,B01002_001E,B27001_001E,B17001_002E",
        "for": "state:*",
        "key": CENSUS_API_KEY
    }

    response = requests.get(CENSUS_BASE_URL, params=params)
    response.raise_for_status()

    raw = response.json()

    headers = [col.lower() for col in raw[0]]
    rows = raw[1:]

    return [dict(zip(headers, row)) for row in rows]

def load_to_snowflake(demographics, conn):
    logger.info(f"Loading {len(demographics)} rows to Snowflake...")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE RAW.CENSUS_STATE_DEMOGRAPHICS")
    logger.info("Table truncated, loading fresh data...")

    rows = [
        tuple(None if value is None else value for value in row.values())
        for row in demographics.iter_rows(named=True)
    ]

    cursor.executemany("""
        INSERT INTO RAW.CENSUS_STATE_DEMOGRAPHICS VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
    """, rows)

    conn.commit()
    logger.info("Load Complete!")

def main():
    logger.info("Starting Census Ingestion...")

    conn = get_snowflake_connection()

    data = fetch_census_data()

    demographics = pl.DataFrame(data)
    demographics = demographics.select(EXPECTED_COLUMNS)

    load_to_snowflake(demographics, conn)

    conn.close()
    logger.info(f"Census ingestion complete! {len(demographics)} rows loaded.")

if __name__ == "__main__":
    main()
