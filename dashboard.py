import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import CallEvent, Anomaly, ENGINE
from datetime import datetime, timedelta

# Page Config
st.set_page_config(
    page_title="Contact Center Monitor",
    page_icon="📡",
    layout="wide"
)

# Title
st.title("📡 Real-Time Contact Center Monitoring")

# CSS for better UI
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #333;
    }
    .alert-box {
        background-color: #4a1c1c;
        color: #ff9999;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Auto-refresh
placeholder = st.empty()

def load_data():
    with ENGINE.connect() as conn:
        # Fetch recent events (last 1 hour)
        query = "SELECT * FROM call_events ORDER BY timestamp DESC LIMIT 100"
        df_events = pd.read_sql(query, conn)
        
        # Fetch recent anomalies
        query_anomalies = "SELECT * FROM anomalies ORDER BY timestamp DESC LIMIT 20"
        df_anomalies = pd.read_sql(query_anomalies, conn)
        
        return df_events, df_anomalies

def calculate_metrics(df_events):
    if df_events.empty:
        return 0, 0, 0
        
    total_calls = len(df_events)
    avg_duration = df_events[df_events['status'] == 'completed']['duration'].mean()
    drop_rate = (len(df_events[df_events['status'] == 'dropped']) / total_calls) * 100 if total_calls > 0 else 0
    
    return total_calls, avg_duration, drop_rate

while True:
    df_events, df_anomalies = load_data()
    total, aht, drop_rate = calculate_metrics(df_events)
    
    with placeholder.container():
        # KPI Row
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Total Calls (Last 100)", f"{total}")
        kpi2.metric("Average Handle Time (AHT)", f"{aht:.1f} sec")
        kpi3.metric("Drop Rate", f"{drop_rate:.1f}%")
        
        # Anomalies Section
        st.subheader("🚨 Recent Alerts")
        if not df_anomalies.empty:
            for _, row in df_anomalies.iterrows():
                st.markdown(f"<div class='alert-box'><b>{row['timestamp']}</b>: {row['description']}</div>", unsafe_allow_html=True)
        else:
            st.info("No active alerts.")
            
        # Charts Row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Call Status Distribution")
            if not df_events.empty:
                status_counts = df_events['status'].value_counts()
                st.bar_chart(status_counts)
        
        with col2:
            st.subheader("Recent AHT Trend")
            if not df_events.empty:
                completed_calls = df_events[df_events['status'] == 'completed']
                if not completed_calls.empty:
                    st.line_chart(completed_calls[['timestamp', 'duration']].set_index('timestamp'))
        
        # Raw Data
        with st.expander("View Raw Event Data"):
            st.dataframe(df_events)
            
    # Refresh rate
    time.sleep(2)
