{{ config(materialized='table') }}

SELECT 
    'hospital_metrics' as source_name,
    MAX(week_ending_date) as last_updated,
    DATEDIFF('day', MAX(week_ending_date), CURRENT_DATE) as days_since_update
FROM {{ ref('stg_hospital_metrics') }}

UNION ALL

SELECT 
    'vaccination_trends' as source_name,
    MAX(vaccination_date) as last_updated,
    DATEDIFF('day', MAX(vaccination_date), CURRENT_DATE) as days_since_update
FROM {{ ref('stg_vaccination_trends') }}

UNION ALL

SELECT 
    'vaccination_coverage' as source_name,
    MAX(week_ending) as last_updated,
    DATEDIFF('day', MAX(week_ending), CURRENT_DATE) as days_since_update
FROM {{ ref('stg_vaccination_coverage') }}

UNION ALL

SELECT 
    'census_state_demographics' as source_name,
    CURRENT_DATE as last_updated,
    0 as days_since_update