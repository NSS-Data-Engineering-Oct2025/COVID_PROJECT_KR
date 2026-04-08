from pathlib import Path

import pendulum
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="covid_pipeline",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "covid_pipeline",
            "schema": "MARTS",
            "warehouse": "COMPUTE_WH",
            "role": "covid_project_role",
        },
    ),
)

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "marts"],
)
def dbt_marts_dag():
    DbtTaskGroup(
        group_id="dbt_marts",
        project_config=ProjectConfig(
            dbt_project_path=Path("/opt/airflow/workspace/covid_pipeline"),
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["marts"]
        ),
    )

dbt_marts_dag()
