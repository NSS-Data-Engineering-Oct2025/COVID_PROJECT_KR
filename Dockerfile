FROM apache/airflow:latest

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-snowflake \
    requests \
    pandas \
    loguru \
    pydantic-settings