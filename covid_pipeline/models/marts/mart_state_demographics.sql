{{ config(materialized='table') }}

with hosp_vax_23_26 as (
    
    select
        state_name,
        covid_season,
        week_ending_date,
        vaccination_coverage_pct,
        total_covid_new_admissions_per_100k,
        CASE WHEN week_ending_date >= '2025-01-20' THEN 'post_jan_2025' ELSE 'pre_jan_2025' END as admin_period
    from {{ ref('int_hospital_vaccination_2023_2026') }}
    WHERE indicator_category_label = 'Received a vaccination'
), census as (
    select
        state_name,
        state_abbrev,
        state_fips_code,
        total_population,
        median_household_income,
        median_age,
        health_insurance_coverage,
        population_below_poverty
    from {{ ref('int_state_demographics') }}
)
select
    vax.covid_season,
    vax.week_ending_date,
    vax.vaccination_coverage_pct,
    vax.total_covid_new_admissions_per_100k,
    vax.admin_period,
    census.state_name,
    census.state_abbrev,
    census.state_fips_code,
    census.total_population,
    census.median_household_income,
    census.median_age,
    census.health_insurance_coverage,
    census.population_below_poverty
from hosp_vax_23_26 as vax
left join census on vax.state_name = census.state_name