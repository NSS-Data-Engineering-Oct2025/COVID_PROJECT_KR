{{config(materialized='view')}}

SELECT 
    census.state_name, 
    LPAD (CAST (fips.st AS VARCHAR), 2, '0') AS state_fips_code, 
    census.total_population, 
    fips.stusps
FROM {{ ref('stg_census_state_demographics') }} AS census
FULL OUTER JOIN {{ ref('fips_state_ref') }} AS fips
ON census.state_fips_code = LPAD(CAST(fips.st AS VARCHAR), 2, '0')