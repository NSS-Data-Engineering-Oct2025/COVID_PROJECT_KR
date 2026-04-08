import streamlit as st
import snowflake.connector
import pandas as pd

@st.cache_resource
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        role=st.secrets["snowflake"]["role"],
        schema=st.secrets["snowflake"]["schema"]
    )
    return conn

@st.cache_data
def run_query(query):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df
    finally:
        cursor.close()