FROM apache/airflow:latest

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-snowflake \
    astronomer-cosmos \
    requests \
    pandas \
    polars \
    pendulum \
    loguru \
    pydantic-settings