{{ config(materialized='table') }}

with source_data as (

    select distinct
        NAME,
        TRY_CAST(B01003_001E AS FLOAT) AS B01003_001E, -- Total Population
        TRY_CAST(B19013_001E AS FLOAT) AS B19013_001E, -- Median Household Income
        TRY_CAST(B01002_001E AS FLOAT) AS B01002_001E, -- Median Age
        TRY_CAST(B27001_001E AS FLOAT) AS B27001_001E, -- Health Insurance Coverage
        TRY_CAST(B17001_002E AS FLOAT) AS B17001_002E, -- Population Below Poverty Level
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