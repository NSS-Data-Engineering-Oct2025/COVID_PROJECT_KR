import pendulum
from airflow.sdk import dag, task

@task
def ingest_hospitalization_data():
    from Ingestion.cdc.ingest_hospitalization import main

    main()

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["covid", "cdc", "hospitalization"],
)

def hospitalization_dag():
    ingest_hospitalization_data()

hospitalization_dag()