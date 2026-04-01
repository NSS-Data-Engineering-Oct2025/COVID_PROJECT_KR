{{ config(materialized='table') }}

with source_data as (

    select distinct
        DATE,
        DATE_TYPE,
        MMWR_WEEK,
        LOCATION,
        ADMINISTERED_DAILY,
        ADMINISTERED_CUMULATIVE,
        ADMIN_DOSE_1_DAILY,
        ADMIN_DOSE_1_CUMULATIVE,
        ADMINISTERED_DOSE1_POP_PCT,
        SERIES_COMPLETE_DAILY,
        SERIES_COMPLETE_CUMULATIVE,
        SERIES_COMPLETE_POP_PCT,
        BOOSTER_DAILY,
        BOOSTER_CUMULATIVE,
        ADDITIONAL_DOSES_VAX_PCT,
        SECOND_BOOSTER_50PLUS_DAILY,
        SECOND_BOOSTER_50PLUS_CUMULATIVE,
        SECOND_BOOSTER_50PLUS_VAX_PCT,
        BIVALENT_BOOSTER_DAILY,
        BIVALENT_BOOSTER_CUMULATIVE,
        BIVALENT_BOOSTER_POP_PCT
    from {{ source('covid_raw', 'VACCINATION_TRENDS') }}
    where DATE_TYPE = 'Admin'
)

select 
	DATE as vaccination_date,
    DATE_TYPE as date_type,
    MMWR_WEEK as mmwr_week,
    LOCATION as state_abbrev,
    ADMINISTERED_DAILY as administered_daily,
    ADMINISTERED_CUMULATIVE as administered_cumulative,
    ADMIN_DOSE_1_DAILY as admin_dose_1_daily,
    ADMIN_DOSE_1_CUMULATIVE as admin_dose_1_cumulative,
    ADMINISTERED_DOSE1_POP_PCT as administered_dose1_pop_pct,
    SERIES_COMPLETE_DAILY as series_complete_daily,
    SERIES_COMPLETE_CUMULATIVE as series_complete_cumulative,
    SERIES_COMPLETE_POP_PCT as series_complete_pop_pct,
    BOOSTER_DAILY as booster_daily,
    BOOSTER_CUMULATIVE as booster_cumulative,
    ADDITIONAL_DOSES_VAX_PCT as additional_doses_vax_pct,
    SECOND_BOOSTER_50PLUS_DAILY as second_booster_50plus_daily,
    SECOND_BOOSTER_50PLUS_CUMULATIVE as second_booster_50plus_cumulative,
    SECOND_BOOSTER_50PLUS_VAX_PCT as second_booster_50plus_vax_pct,
    BIVALENT_BOOSTER_DAILY as bivalent_booster_daily,
    BIVALENT_BOOSTER_CUMULATIVE as bivalent_booster_cumulative,
    BIVALENT_BOOSTER_POP_PCT as bivalent_booster_pop_pct
from source_data