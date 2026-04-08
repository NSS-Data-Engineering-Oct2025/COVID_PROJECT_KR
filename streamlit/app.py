import streamlit as st

st.title("COVID-19: Vaccination & Hospitalization Analytics")

st.markdown("""
This dashboard presents an integrated analysis of COVID-19 vaccination and 
hospitalization trends across the United States, built on a production data 
pipeline using Python, Snowflake, dbt, and Apache Airflow.

**Data Sources:**
- CDC Weekly Hospital Respiratory Data (NHSN), 2020–2026
- CDC COVID-19 Vaccination Trends, 2020–2023
- CDC COVID-19 Vaccination Coverage by Demographics, 2023–2026
- US Census ACS 5-Year Demographic Data

**Analytical Stories:**
- **Vaccination Rollout vs. Hospitalization Decline** (2020–2023)
- **Seasonal Booster Uptake vs. Hospitalizations** (2023–2026)
- **State Demographics & Vaccination Correlation**
- **Data Quality Dashboard**

Use the sidebar to navigate between analyses.
""")