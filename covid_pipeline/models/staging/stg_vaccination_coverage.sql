{{ config(materialized='table') }}

with source_data as (

    select distinct
        VACCINE,
        GEOGRAPHIC_LEVEL,
        GEOGRAPHIC_NAME,
        DEMOGRAPHIC_LEVEL,
        DEMOGRAPHIC_NAME,
        INDICATOR_LABEL,
        INDICATOR_CATEGORY_LABEL,
        MONTH_WEEK,
        TRY_CAST(WEEK_ENDING AS DATE) AS WEEK_ENDING,
        TRY_CAST(ESTIMATE AS FLOAT) AS ESTIMATE,
        TRY_CAST(CI_HALF_WIDTH_95PCT AS FLOAT) AS CI_HALF_WIDTH_95PCT,
        TRY_CAST(UNWEIGHTED_SAMPLE_SIZE AS FLOAT) AS UNWEIGHTED_SAMPLE_SIZE,
        CURRENT_SEASON_WEEK_ENDING,
        COVID_SEASON,
        SUPPRESSION_FLAG
    from {{ source('covid_raw', 'VACCINATION_COVERAGE') }}
    where VACCINE = 'COVID' and GEOGRAPHIC_LEVEL = 'State' -- Filter to only include COVID-19 vaccination data
)

select 
	VACCINE as vaccine,
    GEOGRAPHIC_LEVEL as geographic_level,
    GEOGRAPHIC_NAME as state_name,
    DEMOGRAPHIC_LEVEL as demographic_level,
    DEMOGRAPHIC_NAME as demographic_name,
    INDICATOR_LABEL as indicator_label,
    INDICATOR_CATEGORY_LABEL as indicator_category_label,
    MONTH_WEEK as month_week,
    WEEK_ENDING as week_ending,
    ESTIMATE as estimate,
    CI_HALF_WIDTH_95PCT as ci_half_width_95pct,
    UNWEIGHTED_SAMPLE_SIZE as unweighted_sample_size,
    CURRENT_SEASON_WEEK_ENDING as current_season_week_ending,
    COVID_SEASON as covid_season,
    SUPPRESSION_FLAG as suppression_flag
from source_data
