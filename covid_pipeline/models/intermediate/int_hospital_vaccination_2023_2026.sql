{{ config(materialized='view') }}

with source_data as (

    select
        STATE_NAME as state_name,
        COVID_SEASON as covid_season,
        WEEK_ENDING as week_ending_date,
        CURRENT_SEASON_WEEK_ENDING as current_season_week_ending,
        INDICATOR_LABEL as indicator_label,
        INDICATOR_CATEGORY_LABEL as indicator_category_label,
        ESTIMATE as vaccination_coverage_pct,
        SUPPRESSION_FLAG as suppression_flag
    from {{ ref('stg_vaccination_coverage') }}
    where suppression_flag = '0'
), hospital_data as (
    select
        WEEK_ENDING_DATE as week_ending_date,
        STATE_ABBREV as state_abbrev,
        TOTAL_COVID_HOSPITALIZED as total_covid_hospitalized,
        TOTAL_COVID_NEW_ADMISSIONS as total_covid_new_admissions,
        TOTAL_COVID_NEW_ADMISSIONS_PER_100K as total_covid_new_admissions_per_100k
    from {{ ref('stg_hospital_metrics') }}
), state_data as (
    select
        STATE_NAME as state_name,
        STATE_FIPS_CODE as state_fips_code,
        STUSPS as state_abbrev
    from {{ ref('int_state_spine') }}
)

select
    vax.state_name,
    vax.covid_season,
    vax.week_ending_date,
    vax.current_season_week_ending,
    vax.indicator_label,
    vax.indicator_category_label,
    vax.vaccination_coverage_pct,
    hosp.total_covid_hospitalized,
    hosp.total_covid_new_admissions,
    hosp.total_covid_new_admissions_per_100k
from source_data as vax
left join state_data as spine on vax.state_name = spine.state_name
left join hospital_data as hosp on spine.state_abbrev = hosp.state_abbrev 
    and vax.week_ending_date = hosp.week_ending_date