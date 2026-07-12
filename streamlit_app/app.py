"""
Streamlit dashboard — Gold layer analytics (Task 2.4)
======================================================
Charts:
  1. Device activity map (device locations, coloured by severity)
  2. Time-series AQI trend (per device, over time)
  3. Top-N devices by average AQI (trailing 24h)

Auto-refreshes every 30 seconds.

Run:
    pip install -r requirements.txt
    streamlit run app.py
"""
import time

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st

st.set_page_config(page_title="IoT Hackathon — Gold Dashboard", layout="wide")

REFRESH_SECONDS = 30


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        role=st.secrets["snowflake"].get("role", "ANALYTICS_ROLE"),
        warehouse=st.secrets["snowflake"].get("warehouse", "HACKATHON_WH"),
        database=st.secrets["snowflake"].get("database", "HACKATHON_IOT"),
        schema=st.secrets["snowflake"].get("schema", "ANALYTICS"),
    )


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def load_latest_positions() -> pd.DataFrame:
    return run_query("""
        select device_id, last_latitude as latitude, last_longitude as longitude,
               avg_aqi_24h, critical_events_24h
        from ANALYTICS.gold_top_devices
    """)


def load_aqi_trend() -> pd.DataFrame:
    return run_query("""
        select device_id, event_date, avg_aqi, max_aqi, critical_events, warning_events
        from ANALYTICS.gold_daily_device_agg
        order by event_date
    """)


def load_top_devices(n: int = 10) -> pd.DataFrame:
    return run_query(f"""
        select device_id, avg_aqi_24h, max_aqi_24h, critical_events_24h, aqi_rank
        from ANALYTICS.gold_top_devices
        order by aqi_rank
        limit {n}
    """)


# --------------------------------------------------------------------------
# Layout
# --------------------------------------------------------------------------
st.title("🌐 IoT Fleet — Gold Layer Dashboard")
st.caption(
    f"Data path: Postgres (WAL) → Debezium CDC → Kafka MSK → Snowflake Bronze "
    f"→ dbt Silver/Gold → this dashboard. Auto-refreshes every {REFRESH_SECONDS}s."
)

placeholder = st.empty()

with placeholder.container():
    col1, col2 = st.columns([2, 1])

    # 1) Device activity map -----------------------------------------------
    with col1:
        st.subheader("📍 Device Activity Map")
        positions = load_latest_positions()
        if not positions.empty:
            fig_map = px.scatter_mapbox(
                positions,
                lat="latitude", lon="longitude",
                color="avg_aqi_24h", size="critical_events_24h",
                hover_name="device_id",
                color_continuous_scale="RdYlGn_r",
                zoom=12, height=420,
                mapbox_style="carto-positron",
            )
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No device position data yet — waiting for pipeline to land Gold data.")

    # 3) Top-N devices --------------------------------------------------
    with col2:
        st.subheader("🏆 Top 10 Devices by AQI (24h)")
        top = load_top_devices(10)
        if not top.empty:
            st.dataframe(top, use_container_width=True, hide_index=True)
        else:
            st.info("No ranking data yet.")

    # 2) AQI time-series trend ----------------------------------------------
    st.subheader("📈 AQI Trend Over Time")
    trend = load_aqi_trend()
    if not trend.empty:
        fig_trend = px.line(
            trend, x="event_date", y="avg_aqi", color="device_id",
            markers=True, height=380,
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No trend data yet.")

    st.caption(f"Last refreshed: {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}")

# --------------------------------------------------------------------------
# Auto-refresh every 30 seconds
# --------------------------------------------------------------------------
time.sleep(REFRESH_SECONDS)
st.rerun()
