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
            "schema": "STAGING",
            "warehouse": "COMPUTE_WH",
            "role": "covid_project_role",
        },
    ),
)

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "staging", "cdc", "vaccination", "trends"],
)

def stg_vaccination_trends_dag():
    DbtTaskGroup(
        group_id="dbt_stg_vaccination_trends",
        project_config=ProjectConfig("/opt/airflow/workspace/covid_pipeline"),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["stg_vaccination_trends"]
        ),
    )

stg_vaccination_trends_dag()
