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
VACC_TRENDS_URL = "https://data.cdc.gov/resource/rh2h-3yt2.json"

BATCH_SIZE = 30000

EXPECTED_COLUMNS = [
    "date",
    "date_type",
    "mmwr_week",
    "location",
    "administered_daily",
    "administered_cumulative",
    "admin_dose_1_daily",
    "admin_dose_1_cumulative",
    "administered_dose1_pop_pct",
    "series_complete_daily",
    "series_complete_cumulative",
    "series_complete_pop_pct",
    "booster_daily",
    "booster_cumulative",
    "additional_doses_vax_pct",
    "second_booster_50plus_daily",
    "second_booster_50plus_cumulative",
    "second_booster_50plus_vax_pct",
    "bivalent_booster_daily",
    "bivalent_booster_cumulative",
    "bivalent_booster_pop_pct",
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

def fetch_vaccination_trends(offset):
    logger.info(f"Fetching vaccination trends at offset {offset}...")

    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$$app_token": CDC_API_TOKEN,
    }

    response = requests.get(VACC_TRENDS_URL, params=params)
    response.raise_for_status()

    return response.json()

def load_to_snowflake(vaccinations, conn):
    logger.info(f"Loading {len(vaccinations)} rows to Snowflake...")

    cursor = conn.cursor()

    for col in EXPECTED_COLUMNS:
        if col not in vaccinations.columns:
            vaccinations = vaccinations.with_columns(pl.lit(None).alias(col))

    vaccinations = vaccinations.select(EXPECTED_COLUMNS)

    rows = [
        tuple(None if value is None else value for value in row.values())
        for row in vaccinations.iter_rows(named=True)
    ]

    cursor.executemany(f"""
        INSERT INTO RAW.VACCINATION_TRENDS VALUES (
            {', '.join(['%s'] * len(EXPECTED_COLUMNS))}
        )
    """, rows)  # noqa: S608

    conn.commit()
    logger.info("Load Complete!")

def main():
    logger.info("Starting vaccination trends ingestion...")

    conn = get_snowflake_connection()

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE RAW.VACCINATION_TRENDS")
    logger.info("Table truncated, loading fresh data...")

    offset = 0
    total_rows = 0

    while True:
        data = fetch_vaccination_trends(offset)

        if not data:
            logger.info("No more data to fetch. Ingestion complete!")
            break

        vaccinations = pl.DataFrame(data)
        load_to_snowflake(vaccinations, conn)

        total_rows += len(vaccinations)
        offset += BATCH_SIZE
        logger.info(f"Total rows loaded so far: {total_rows}")

    conn.close()
    logger.info(f"Ingestion complete! Total rows loaded: {total_rows}")

if __name__ == "__main__":
    main()
