{{ config(materialized='view') }}

with source_data as (

    select
        STATE_ABBREV as state_abbrev,
        MMWR_WEEK as mmwr_week,
        MAX(VACCINATION_DATE) as week_ending_date,
        SUM(ADMINISTERED_DAILY) as weekly_doses_administered,
        SUM(ADMIN_DOSE_1_DAILY) as weekly_dose_1,
        SUM(SERIES_COMPLETE_DAILY) as weekly_series_complete,
        SUM(BOOSTER_DAILY) as weekly_boosters,
        SUM(BIVALENT_BOOSTER_DAILY) as weekly_bivalent_boosters,
        MAX(ADMINISTERED_CUMULATIVE) as cumulative_doses,
        MAX(SERIES_COMPLETE_POP_PCT) as series_complete_pop_pct
    from {{ ref('stg_vaccination_trends') }}
    GROUP BY STATE_ABBREV, MMWR_WEEK
), hospital_data as (
    select
        WEEK_ENDING_DATE as week_ending_date,
        STATE_ABBREV as state_abbrev,
        TOTAL_COVID_HOSPITALIZED as total_covid_hospitalized,
        TOTAL_COVID_NEW_ADMISSIONS as total_covid_new_admissions,
        TOTAL_COVID_NEW_ADMISSIONS_PER_100K as total_covid_new_admissions_per_100k,
        NUM_ICU_BEDS as num_icu_beds,
        NUM_ICU_BEDS_OCC as num_icu_beds_occ,
        NUM_INPATIENT_BEDS as num_inpatient_beds,
        NUM_INPATIENT_BEDS_OCC as num_inpatient_beds_occ,
        COVID_HOSPITALIZED_ADULTS as covid_hosp_adults,
        COVID_HOSPITALIZED_PED as covid_hosp_ped
    from {{ ref('stg_hospital_metrics') }}
)


select 
        vax.state_abbrev,
        vax.mmwr_week,
        vax.week_ending_date,
        vax.weekly_doses_administered,
        vax.weekly_dose_1,
        vax.weekly_series_complete,
        vax.weekly_boosters,
        vax.weekly_bivalent_boosters,
        vax.cumulative_doses,
        vax.series_complete_pop_pct,
        hosp.total_covid_hospitalized,
        hosp.total_covid_new_admissions,
        hosp.total_covid_new_admissions_per_100k,
        hosp.num_icu_beds,
        hosp.num_icu_beds_occ,
        hosp.num_inpatient_beds,
        hosp.num_inpatient_beds_occ,
        hosp.covid_hosp_adults,
        hosp.covid_hosp_ped
from source_data as vax
inner join hospital_data as hosp
    on vax.state_abbrev = hosp.state_abbrev 
    and vax.week_ending_date = hosp.week_ending_date