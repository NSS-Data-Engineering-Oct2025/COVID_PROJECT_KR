{{ config(materialized='view') }}

with census_data as (

    select
        STATE_FIPS_CODE as state_fips_code,
        STATE_NAME as state_name,
        TOTAL_POPULATION as total_population,
        MEDIAN_HOUSEHOLD_INCOME as median_household_income,
        MEDIAN_AGE as median_age,
        HEALTH_INSURANCE_COVERAGE as health_insurance_coverage,
        POPULATION_BELOW_POVERTY_LEVEL as population_below_poverty
    from {{ ref('stg_census_state_demographics') }}
), spine_data as (
    select
        STATE_NAME as state_name,
        STUSPS as state_abbrev,
        STATE_FIPS_CODE as state_fips_code
    from {{ ref('int_state_spine') }}
)


select 
    spine.state_fips_code,
    spine.state_name, 
    spine.state_abbrev,
    census.total_population,
    census.median_household_income,
    census.median_age,
    census.health_insurance_coverage,
    census.population_below_poverty   
from spine_data as spine
inner join census_data as census
    on spine.state_fips_code = census.state_fips_code
