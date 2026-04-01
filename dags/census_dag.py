import pendulum
from airflow.sdk import dag, task

@task
def ingest_census_data():
    from Ingestion.census.ingest_census import main

    main()

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["census", "population", "demographics"],
)

def census_dag():
    ingest_census_data()

census_dag()