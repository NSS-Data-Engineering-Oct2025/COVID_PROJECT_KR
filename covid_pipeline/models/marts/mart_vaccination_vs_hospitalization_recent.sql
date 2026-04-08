{{ config(materialized='table') }}

with hosp_vax_23_26 as (
    select
        state_name,
        covid_season,
        week_ending_date,
        current_season_week_ending,
        indicator_label,
        indicator_category_label,
        vaccination_coverage_pct,
        total_covid_hospitalized,
        total_covid_new_admissions,
        total_covid_new_admissions_per_100k,
        total_covid_icu_patients,
        num_icu_beds,
        num_icu_beds_occ,
        num_inpatient_beds,
        num_inpatient_beds_occ,
        covid_hosp_adults,
        covid_hosp_ped,
        resp_season,
        num_icu_beds_occ / NULLIF(num_icu_beds, 0) as icu_occ_rate,
        num_inpatient_beds_occ / NULLIF(num_inpatient_beds, 0) as inpatient_occ_rate,
        total_covid_icu_patients / NULLIF(num_icu_beds, 0) AS covid_icu_occ_rate,
        total_covid_hospitalized / NULLIF(num_inpatient_beds, 0) AS covid_inpatient_occ_rate,
        CASE WHEN week_ending_date >= '2025-01-20' THEN 'post_jan_2025' ELSE 'pre_jan_2025' END as admin_period
    from {{ ref('int_hospital_vaccination_2023_2026') }}
)
select
    state_name,
    covid_season,
    week_ending_date,
    current_season_week_ending,
    indicator_label,
    indicator_category_label,
    vaccination_coverage_pct,
    total_covid_hospitalized,
    total_covid_new_admissions,
    total_covid_new_admissions_per_100k,
    total_covid_icu_patients,
    num_icu_beds,
    num_icu_beds_occ,
    num_inpatient_beds,
    num_inpatient_beds_occ,
    covid_hosp_adults,
    covid_hosp_ped,
    resp_season,
    icu_occ_rate,
    inpatient_occ_rate,
    covid_icu_occ_rate,
    covid_inpatient_occ_rate,
    admin_period
from hosp_vax_23_26