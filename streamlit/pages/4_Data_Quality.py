import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import run_query

import streamlit as st

st.title("Data Quality Dashboard")
st.markdown("Pipeline health and data freshness by source.")

query = """
    SELECT *
    FROM mart_data_quality
    ORDER BY days_since_update DESC
"""

data = run_query(query)

st.dataframe(data, use_container_width=True)