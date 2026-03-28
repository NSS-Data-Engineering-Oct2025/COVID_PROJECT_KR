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
HOSP_BASE_URL = "https://data.cdc.gov/resource/mpgq-jmmr.json"

BATCH_SIZE = 10000

EXPECTED_COLUMNS = [
    "weekendingdate",
    "jurisdiction",
    "numinptbeds",
    "numinptbedsadult",
    "numinptbedsped",
    "numinptbedsocc",
    "numinptbedsoccadult",
    "numinptbedsoccped",
    "numicubeds",
    "numicubedsadult",
    "numicubedsped",
    "numicubedsocc",
    "numicubedsoccadult",
    "numicubedsoccped",
    "numconfc19hosppatsadult",
    "numconfc19hosppatsped",
    "totalconfc19hosppats",
    "totalconffluhosppats",
    "numconfc19icupatsadult",
    "numconfc19icupatsped",
    "totalconfc19icupats",
    "totalconffluicupats",
    "numconfc19newadmped0to4",
    "numconfc19newadmped5to17",
    "totalconfc19newadmped",
    "numconfc19newadmadult18to49",
    "totalconfc19newadmadult",
    "numconfc19newadmunk",
    "totalconfc19newadm",
    "totalconfflunewadm",
    "pctinptbedsocc",
    "pctconfc19inptbeds",
    "pctconffluinptbeds",
    "pcticubedsocc",
    "pctconfc19icubeds",
    "pctconffluicubeds",
    "pctconfc19newadmadult",
    "pctconfc19newadmped",
    "numinptbedshosprep",
    "numinptbedsocchosprep",
    "numicubedshosprep",
    "numicubedsocchosprep",
    "totalconfc19hosppatshosprep",
    "totalconffluhosppatshosprep",
    "totalconfrsvhosppatshosprep",
    "totalconfc19icupatshosprep",
    "totalconffluicupatshosprep",
    "totalconfrsvicupatshosprep",
    "totalconfc19newadmpedhosprep",
    "totalconfc19newadmadulthosprep",
    "totalconfc19newadmhosprep",
    "totalconfflunewadmpedhosprep",
    "totalconfflunewadmadulthosprep",
    "totalconfflunewadmhosprep",
    "totalconfrsvnewadmpedhosprep",
    "totalconfrsvnewadmadulthosprep",
    "totalconfrsvnewadmhosprep",
    "pctinptbedsocchosprep",
    "pcticubedsocchosprep",
    "pctconfc19inptbedshosprep",
    "pctconffluinptbedshosprep",
    "pctconfrsvinptbedshosprep",
    "pctconfc19icubedshosprep",
    "pctconffluicubedshosprep",
    "pctconfrsvicubedshosprep",
    "numinptbedsperchosprep",
    "numinptbedsoccperchosprep",
    "numicubedsperchosprep",
    "numicubedsoccperchosprep",
    "totalconfc19hosppatsperc",
    "totalconffluhosppatsperc",
    "totalconfrsvhosppatsperc",
    "totalconfc19icupatsperchosprep",
    "totalconffluicupatsperchosprep",
    "totalconfrsvicupatsperchosprep",
    "totalconfc19newadmpedper",
    "totalconfc19newadmadultp",
    "totalconfc19newadmperchosprep",
    "totalconfflunewadmpedper",
    "totalconfflunewadmadultp",
    "totalconfflunewadmperchosprep",
    "totalconfrsvnewadmpedper",
    "totalconfrsvnewadmadultp",
    "totalconfrsvnewadmperchosprep",
    "pctinptbedsoccperchosprep",
    "pcticubedsoccperchosprep",
    "pctconfc19inptbedsperchosprep",
    "pctconffluinptbedsperchosprep",
    "pctconfrsvinptbedsperchosprep",
    "pctconfc19icubedsperchosprep",
    "pctconffluicubedsperchosprep",
    "pctconfrsvicubedsperchosprep",
    "numinptbedsperchosprepabschg",
    "numinptbedsoccperchospre",
    "numicubedsperchosprepabschg",
    "numicubedsoccperchosprepabschg",
    "totalconfc19hosppatsperc_1",
    "totalconffluhosppatsperc_1",
    "totalconfrsvhosppatsperc_1",
    "totalconfc19icupatsperch",
    "totalconffluicupatsperch",
    "totalconfrsvicupatsperch",
    "totalconfc19newadmpedper_1",
    "totalconfc19newadmadultp_1",
    "totalconfc19newadmpercho",
    "totalconfflunewadmpedper_1",
    "totalconfflunewadmadultp_1",
    "totalconfflunewadmpercho",
    "totalconfrsvnewadmpedper_1",
    "totalconfrsvnewadmadultp_1",
    "totalconfrsvnewadmpercho",
    "pctinptbedsoccperchospre",
    "pcticubedsoccperchosprepabschg",
    "pctconfc19inptbedspercho",
    "pctconffluinptbedspercho",
    "pctconfrsvinptbedspercho",
    "pctconfc19icubedsperchos",
    "pctconffluicubedsperchos",
    "pctconfrsvicubedsperchos",
    "numconfc19newadmped0to4per100k",
    "numconfc19newadmped5to17per100k",
    "totalconfc19newadmpedper100k",
    "numconfc19newadmadult18to49per100k",
    "totalconfc19newadmadultper100k",
    "totalconfc19newadmper100k",
    "totalconfflunewadmper100k",
    "totalconfc19newadmperchosprepabove80pct",
    "totalconfc19newadmperchosprepabove90pct",
    "totalconfflunewadmperchosprepabove80pct",
    "totalconfflunewadmperchosprepabove90pct",
    "totalconfrsvnewadmperchosprepabove80pct",
    "totalconfrsvnewadmperchosprepabove90pct",
    "pctinptbedsoccadult",
    "pctinptbedsoccped",
    "pcticubedsoccadult",
    "pcticubedsoccped",
    "pctconfc19inptbedsadult",
    "pctconfc19inptbedsped",
    "pctconfc19icubedsadult",
    "pctconfc19icubedsped",
    "pctconfc19hosppatsicu",
    "pctconfc19hosppatsicuadult",
    "pctconfc19hosppatsicuped",
    "pctconffluhosppatsicu",
    "numconfc19newadmped0to4pctchg",
    "totalconfc19newadmpedpctchg",
    "numconfc19newadmadult18to49pctchg",
    "totalconfc19newadmadultpctchg",
    "totalconfc19newadmpctchg",
    "totalconfflunewadmpctchg",
    "pctinptbedsoccadulthosprep",
    "pctinptbedsoccpedhosprep",
    "pcticubedsoccadulthosprep",
    "pcticubedsoccpedhosprep",
    "pctconfc19inptbedsadulthosprep",
    "pctconfc19inptbedspedhosprep",
    "pctconfc19icubedsadulthosprep",
    "pctconfc19icubedspedhosprep",
    "pctconffluinptbedsadulthosprep",
    "pctconffluinptbedspedhosprep",
    "pctconffluicubedsadulthosprep",
    "pctconffluicubedspedhosprep",
    "pctconfrsvinptbedsadulthosprep",
    "pctconfrsvinptbedspedhosprep",
    "pctconfrsvicubedsadulthosprep",
    "pctconfrsvicubedspedhosprep",
    "pctconfc19hosppatsicuhosprep",
    "pctconfc19hosppatsicuadulthosprep",
    "pctconfc19hosppatsicupedhosprep",
    "pctconffluhosppatsicuhosprep",
    "pctconffluhosppatsicuadulthosprep",
    "pctconffluhosppatsicupedhosprep",
    "pctconfrsvhosppatsicuhosprep",
    "pctconfrsvhosppatsicuadulthosprep",
    "pctconfrsvhosppatsicupedhosprep",
    "pctinptbedsoccadultperchosprep",
    "pctinptbedsoccpedperchosprep",
    "pcticubedsoccadultperchosprep",
    "pcticubedsoccpedperchosprep",
    "pctconfc19inptbedsadultperchosprep",
    "pctconfc19inptbedspedperchosprep",
    "pctconfc19icubedsadultperchosprep",
    "pctconfc19icubedspedperchosprep",
    "pctconffluinptbedsadultperchosprep",
    "pctconffluinptbedspedperchosprep",
    "pctconffluicubedsadultperchosprep",
    "pctconffluicubedspedperchosprep",
    "pctconfrsvinptbedsadultperchosprep",
    "pctconfrsvinptbedspedperchosprep",
    "pctconfrsvicubedsadultperchosprep",
    "pctconfrsvicubedspedperchosprep",
    "pctconfc19hosppatsicuperchosprep",
    "pctconfc19hosppatsicuadultperchosprep",
    "pctconfc19hosppatsicupedperchosprep",
    "pctconffluhosppatsicuperchosprep",
    "pctconffluhosppatsicuadultperchosprep",
    "pctconffluhosppatsicupedperchosprep",
    "pctconfrsvhosppatsicuperchosprep",
    "pctconfrsvhosppatsicuadultperchosprep",
    "pctconfrsvhosppatsicupedperchosprep",
    "pctinptbedsoccadultperchosprepabschg",
    "pctinptbedsoccpedperchosprepabschg",
    "pcticubedsoccadultperchosprepabschg",
    "pcticubedsoccpedperchosprepabschg",
    "pctconfc19inptbedsadultperchosprepabschg",
    "pctconfc19inptbedspedperchosprepabschg",
    "pctconfc19icubedsadultperchosprepabschg",
    "pctconfc19icubedspedperchosprepabschg",
    "pctconffluinptbedsadultperchosprepabschg",
    "pctconffluinptbedspedperchosprepabschg",
    "pctconffluicubedsadultperchosprepabschg",
    "pctconffluicubedspedperchosprepabschg",
    "pctconfrsvinptbedsadultperchosprepabschg",
    "pctconfrsvinptbedspedperchosprepabschg",
    "pctconfrsvicubedsadultperchosprepabschg",
    "pctconfrsvicubedspedperchosprepabschg",
    "pctconfc19hosppatsicuperchosprepabschg",
    "pctconfc19hosppatsicuadultperchosprepabschg",
    "pctconfc19hosppatsicupedperchosprepabschg",
    "pctconffluhosppatsicuperchosprepabschg",
    "pctconffluhosppatsicuadultperchosprepabschg",
    "pctconffluhosppatsicupedperchosprepabschg",
    "pctconfrsvhosppatsicuperchosprepabschg",
    "pctconfrsvhosppatsicuadultperchosprepabschg",
    "pctconfrsvhosppatsicupedperchosprepabschg",
    "respseason",
    "totalconfc19newadmcumulativeseasonalsum",
    "totalconfflunewadmcumulativeseasonalsum",
    "totalconfrsvnewadmcumulativeseasonalsum",
    "totalconfnewadmcumulativeseasonalsum",
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

def fetch_hospitalization_data(offset):
    logger.info(f"Fetching hospitalization data at offset {offset}...")

    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$$app_token": CDC_API_TOKEN,
    }

    response = requests.get(HOSP_BASE_URL, params=params)
    response.raise_for_status()

    return response.json()

def load_to_snowflake(hospitalizations, conn):
    logger.info(f"Loading {len(hospitalizations)} rows to Snowflake...")

    cursor = conn.cursor()

    for col in EXPECTED_COLUMNS:
        if col not in hospitalizations.columns:
            hospitalizations = hospitalizations.with_columns(pl.lit(None).alias(col))

    hospitalizations = hospitalizations.select(EXPECTED_COLUMNS)

    rows = [
        tuple(None if value is None else value for value in row.values())
        for row in hospitalizations.iter_rows(named=True)
    ]

    cursor.executemany(f"""
        INSERT INTO RAW.HOSPITAL_METRICS VALUES (
            {', '.join(['%s'] * len(EXPECTED_COLUMNS))}
        )
    """, rows)

    conn.commit()
    logger.info("Load complete!")

def main():
    logger.info("Starting hospitalization ingestion...")

    conn = get_snowflake_connection()

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE RAW.HOSPITAL_METRICS")
    logger.info("Table truncated, loading fresh data...")

    offset = 0
    total_rows = 0

    while True:
        data = fetch_hospitalization_data(offset)

        if not data:
            logger.info("No more data to fetch. Ingestion complete!")
            break

        hospitalizations = pl.DataFrame(data)
        load_to_snowflake(hospitalizations, conn)

        total_rows += len(hospitalizations)
        offset += BATCH_SIZE
        logger.info(f"Total rows loaded so far: {total_rows}")

    conn.close()
    logger.info(f"Ingestion complete! Total rows loaded: {total_rows}")


if __name__ == "__main__":
    main()