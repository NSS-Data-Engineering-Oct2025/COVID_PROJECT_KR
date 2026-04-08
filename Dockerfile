FROM apache/airflow:latest

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-snowflake \
    astronomer-cosmos \
    requests \
    pandas \
    loguru \
    pydantic-settings