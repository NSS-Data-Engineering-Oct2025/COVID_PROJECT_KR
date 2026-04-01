{{ config(materialized='table') }}

with source_data as (

    select distinct
        NAME,
        B01003_001E, -- Total Population
        B19013_001E, -- Median Household Income
        B01002_001E, -- Median Age
        B27001_001E, -- Health Insurance Coverage
        B17001_002E, -- Population Below Poverty Level
        STATE
    from {{ source('covid_raw', 'CENSUS_STATE_DEMOGRAPHICS') }}

)

select 
	NAME as state_name,    
	B01003_001E as total_population,
	B19013_001E as median_household_income,
	B01002_001E as median_age,
	B27001_001E as health_insurance_coverage,
	B17001_002E as population_below_poverty_level,
	STATE as state_fips_code
from source_data