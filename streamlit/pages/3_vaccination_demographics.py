import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import run_query

import streamlit as st
import plotly.express as px

st.title("State Demographics & Vaccination Correlation")

query = """
    SELECT  STATE_NAME as state_name,
            STATE_ABBREV as state_abbrev,
            AVG(VACCINATION_COVERAGE_PCT) AS avg_vaccination_coverage_pct,
            AVG(TOTAL_COVID_NEW_ADMISSIONS_PER_100K) AS avg_covid_new_admissions_per_100k,
            MAX(TOTAL_POPULATION) AS total_population,
            MAX(MEDIAN_HOUSEHOLD_INCOME) AS median_household_income,
            MAX(MEDIAN_AGE) AS median_age,
            MAX(HEALTH_INSURANCE_COVERAGE) / NULLIF(MAX(TOTAL_POPULATION), 0) * 100 AS health_insurance_coverage_pct,
            MAX(POPULATION_BELOW_POVERTY) / NULLIF(MAX(TOTAL_POPULATION), 0) * 100 AS poverty_rate_pct
    FROM mart_state_demographics
    GROUP BY state_name, state_abbrev
    ORDER BY state_name
"""

data = run_query(query)

st.subheader("Median Household Income vs. Vaccination Coverage")
fig1 = px.scatter(
    data,
    x='MEDIAN_HOUSEHOLD_INCOME',
    y='AVG_VACCINATION_COVERAGE_PCT',
    text='STATE_ABBREV',
    labels={
        'MEDIAN_HOUSEHOLD_INCOME': 'Median Household Income ($)',
        'AVG_VACCINATION_COVERAGE_PCT': 'Avg Vaccination Coverage (%)'
    },
    title='Does Income Predict Vaccination Coverage?'
)
fig1.update_traces(textposition='top center')
st.plotly_chart(fig1, use_container_width=True)

st.subheader("Health Insurance Coverage vs. Vaccination Coverage")
fig2 = px.scatter(
    data,
    x='HEALTH_INSURANCE_COVERAGE_PCT',
    y='AVG_VACCINATION_COVERAGE_PCT',
    text='STATE_ABBREV',
    labels={
        'HEALTH_INSURANCE_COVERAGE_PCT': 'Health Insurance Coverage (%)',
        'AVG_VACCINATION_COVERAGE_PCT': 'Avg Vaccination Coverage (%)'
    },
    title='Does Insurance Access Predict Vaccination Coverage?'
)
fig2.update_traces(textposition='top center')
st.plotly_chart(fig2, use_container_width=True)

st.subheader("Poverty Rate vs. Hospitalization Rate")
fig3 = px.scatter(
    data,
    x='POVERTY_RATE_PCT',
    y='AVG_COVID_NEW_ADMISSIONS_PER_100K',
    text='STATE_ABBREV',
    labels={
        'POVERTY_RATE_PCT': 'Poverty Rate (%)',
        'AVG_COVID_NEW_ADMISSIONS_PER_100K': 'Avg COVID New Admissions per 100k'
    },
    title='Does Poverty Predict Hospitalization Rate?'
)
fig3.update_traces(textposition='top center')
st.plotly_chart(fig3, use_container_width=True)