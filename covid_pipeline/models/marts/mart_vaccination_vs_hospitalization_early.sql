{{ config(materialized='table') }}

with hosp_vax_20_23 as (
    select
        state_abbrev,
        mmwr_week,
        week_ending_date,
        weekly_doses_administered,
        weekly_dose_1,
        weekly_series_complete,
        weekly_boosters,
        weekly_bivalent_boosters,
        cumulative_doses,
        series_complete_pop_pct,
        total_covid_hospitalized,
        total_covid_new_admissions,
        total_covid_new_admissions_per_100k,
        num_icu_beds,
        num_icu_beds_occ,
        num_inpatient_beds,
        num_inpatient_beds_occ,
        covid_hosp_adults,
        covid_hosp_ped,
        num_icu_beds_occ / NULLIF(num_icu_beds, 0) as icu_occ_rate,
        num_inpatient_beds_occ / NULLIF(num_inpatient_beds, 0) as inpatient_occ_rate,
        total_covid_icu_patients / NULLIF(num_icu_beds, 0) AS covid_icu_occ_rate,
        total_covid_hospitalized / NULLIF(num_inpatient_beds, 0) AS covid_inpatient_occ_rate
    from {{ ref('int_hospital_vaccination_2020_2023') }}
    WHERE week_ending_date <= '2023-05-31'
)
select
    state_abbrev,
    mmwr_week,
    week_ending_date,
    weekly_doses_administered,
    weekly_dose_1,
    weekly_series_complete,
    weekly_boosters,
    weekly_bivalent_boosters,
    cumulative_doses,
    series_complete_pop_pct,
    total_covid_hospitalized,
    total_covid_new_admissions,
    total_covid_new_admissions_per_100k,
    num_icu_beds,
    num_icu_beds_occ,
    num_inpatient_beds,
    num_inpatient_beds_occ,
    covid_hosp_adults,
    covid_hosp_ped,
    icu_occ_rate,
    inpatient_occ_rate,
    covid_icu_occ_rate,
    covid_inpatient_occ_rate
from hosp_vax_20_23