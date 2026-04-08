import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import run_query

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.title("COVID-19 Vaccination Rollout vs. Hospitalization Decline (2020–2023)")

query = """
    SELECT  WEEK_ENDING_DATE AS week_ending_date,
            SUM(WEEKLY_DOSES_ADMINISTERED) AS total_weekly_doses_administered,
            SUM(CUMULATIVE_DOSES) AS total_cumulative_doses,
            AVG(SERIES_COMPLETE_POP_PCT) AS avg_series_complete_pop_pct,
            AVG(TOTAL_COVID_NEW_ADMISSIONS_PER_100K) AS total_covid_new_admissions_per_100k,
            AVG(COVID_INPATIENT_OCC_RATE) AS avg_covid_inpatient_occupancy_rate
    
    FROM mart_vaccination_vs_hospitalization_early
    
    GROUP BY week_ending_date
    
    ORDER BY week_ending_date
"""

data = run_query(query)

# Create dual-axis line chart
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=data['WEEK_ENDING_DATE'],
    y=data['TOTAL_WEEKLY_DOSES_ADMINISTERED'],
    name='Weekly Doses Administered',
    yaxis='y1',
    line=dict(color='#2196F3')  # blue
))

fig.add_trace(go.Scatter(
    x=data['WEEK_ENDING_DATE'],
    y=data['TOTAL_COVID_NEW_ADMISSIONS_PER_100K'],
    name='COVID New Admissions per 100k',
    yaxis='y2',
    line=dict(color='#FF5722')  # orange-red
))

fig.update_layout(
    title='Vaccination Rollout vs. COVID Hospitalizations (2020-2023)',
    xaxis=dict(title='Week'),
    yaxis=dict(title='Weekly Doses Administered', side='left'),
    yaxis2=dict(title='New Admissions per 100k', side='right', overlaying='y'),
    legend=dict(x=0, y=1.1, orientation='h')
)

st.plotly_chart(fig, use_container_width=True)