import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import run_query

import streamlit as st
import plotly.graph_objects as go

st.title("COVID-19 Seasonal Booster Uptake vs. Hospitalizations (2023–2026)")

query = """
    SELECT  WEEK_ENDING_DATE AS week_ending_date,
            COVID_SEASON AS covid_season,
            AVG(VACCINATION_COVERAGE_PCT) AS avg_vaccination_coverage_pct,
            AVG(TOTAL_COVID_NEW_ADMISSIONS_PER_100K) AS avg_covid_new_admissions_per_100k
    FROM mart_vaccination_vs_hospitalization_recent
    WHERE INDICATOR_CATEGORY_LABEL = 'Received a vaccination'
    GROUP BY week_ending_date, covid_season
    ORDER BY week_ending_date
"""

data = run_query(query)

season_colors = {
    '2023-2024': '#2196F3',
    '2024-2025': '#4CAF50',
    '2025-2026': '#FF9800'
}

fig = go.Figure()

for season in data['COVID_SEASON'].unique():
    season_data = data[data['COVID_SEASON'] == season]
    fig.add_trace(go.Scatter(
        x=season_data['WEEK_ENDING_DATE'],
        y=season_data['AVG_VACCINATION_COVERAGE_PCT'],
        name=f'Vaccination Coverage {season}',
        yaxis='y1',
        line=dict(color=season_colors.get(season, '#999999'))
    ))

fig.add_trace(go.Scatter(
    x=data['WEEK_ENDING_DATE'],
    y=data['AVG_COVID_NEW_ADMISSIONS_PER_100K'],
    name='COVID New Admissions per 100k',
    yaxis='y2',
    line=dict(color='#FF5722', dash='dash')
))

fig.update_layout(
    title='Seasonal Booster Uptake vs. COVID Hospitalizations (2023-2026)',
    xaxis=dict(title='Week'),
    yaxis=dict(title='Vaccination Coverage %', side='left'),
    yaxis2=dict(title='New Admissions per 100k', side='right', overlaying='y'),
    legend=dict(x=0, y=1.1, orientation='h')
)

st.plotly_chart(fig, use_container_width=True)