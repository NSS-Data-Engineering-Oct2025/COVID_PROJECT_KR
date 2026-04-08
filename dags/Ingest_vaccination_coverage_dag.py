import pendulum
from airflow.sdk import dag, task


@task
def ingest_vaccination_coverage():
    from Ingestion.cdc.ingest_vaccination_coverage import main

    main()


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["covid", "cdc", "vaccination"],
)

def ingest_vaccination_coverage_dag():
    ingest_vaccination_coverage()


ingest_vaccination_coverage_dag()
