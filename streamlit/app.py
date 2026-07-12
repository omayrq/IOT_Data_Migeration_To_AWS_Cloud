"""
IoT Streaming Platform — Analytics Dashboard
=============================================
Connects to Snowflake Gold layer and renders:
  - Live device map (pydeck)
  - Temperature trends
  - Device health leaderboard
  - Anomaly alerts
  - Data quality metrics
  - Auto-refreshes every 30 seconds
"""

import os
import time
import json
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IoT Streaming Platform",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS — Dark premium theme
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0d1117; }
    .main-header {
        font-size: 2.2rem; font-weight: 700;
        background: linear-gradient(90deg, #00d2ff, #7b2ff7);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e, #242938);
        border: 1px solid #2d3748; border-radius: 12px;
        padding: 1.2rem; text-align: center; box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    .metric-value { font-size: 2.4rem; font-weight: 700; color: #00d2ff; }
    .metric-label { font-size: 0.85rem; color: #8892a4; margin-top: 0.2rem; }
    .alert-critical { background: rgba(239,68,68,0.15); border-left: 4px solid #ef4444; padding: 0.75rem 1rem; border-radius: 4px; margin: 0.3rem 0; }
    .alert-warning  { background: rgba(245,158,11,0.15); border-left: 4px solid #f59e0b; padding: 0.75rem 1rem; border-radius: 4px; margin: 0.3rem 0; }
    .badge-healthy  { background: #064e3b; color: #6ee7b7; padding: 2px 10px; border-radius: 999px; font-size: 0.75rem; }
    .badge-warning  { background: #78350f; color: #fde68a; padding: 2px 10px; border-radius: 999px; font-size: 0.75rem; }
    .badge-critical { background: #7f1d1d; color: #fca5a5; padding: 2px 10px; border-radius: 999px; font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Snowflake Connection (cached)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(ttl=300)
def get_snowflake_connection():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_ANALYST_USER", "ANALYST_USER"),
        password  = os.getenv("SNOWFLAKE_ANALYST_PASSWORD"),
        role      = "IOT_ANALYST_ROLE",
        warehouse = "IOT_ANALYTICS_WH",
        database  = "IOT_GOLD",
        schema    = "KPI",
    )


@st.cache_data(ttl=30)  # Cache for 30 seconds (matches auto-refresh)
def query(_conn, sql: str) -> pd.DataFrame:
    try:
        cursor = _conn.cursor()
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")

    auto_refresh = st.toggle("Auto-refresh (30s)", value=True)
    selected_region = st.selectbox(
        "Filter by Region",
        ["All", "new_york", "los_angeles", "chicago", "houston", "miami"],
    )
    date_range_days = st.slider("Days of history", 1, 30, 7)
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<p class="main-header">📡 IoT Streaming Platform</p>', unsafe_allow_html=True)
st.markdown("Real-time telemetry analytics · Kafka → Debezium → Snowflake → dbt")
st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────────────────────────────────────
conn = get_snowflake_connection()

region_filter = f"AND region = '{selected_region}'" if selected_region != "All" else ""
date_filter   = f"event_date >= CURRENT_DATE - {date_range_days}"

# KPI summary
df_kpi = query(conn, f"""
    SELECT
        COUNT(DISTINCT device_id)           AS total_devices,
        SUM(total_events)                   AS total_events,
        ROUND(AVG(uptime_pct), 1)           AS avg_uptime,
        ROUND(AVG(avg_temperature), 1)      AS avg_temperature,
        SUM(had_critical_battery)           AS critical_battery_devices,
        SUM(had_offline_event)              AS offline_events
    FROM IOT_GOLD.KPI.IOT_DEVICE_SUMMARY
    WHERE {date_filter} {region_filter}
""")

# Latest device status (last seen)
df_devices = query(conn, f"""
    SELECT
        device_id, region,
        last_event_at, uptime_pct,
        avg_temperature, min_battery, avg_signal_strength,
        worst_battery_health, device_health_score,
        had_critical_battery, had_offline_event
    FROM IOT_GOLD.KPI.IOT_DEVICE_SUMMARY
    WHERE event_date = (SELECT MAX(event_date) FROM IOT_GOLD.KPI.IOT_DEVICE_SUMMARY)
      {region_filter}
    ORDER BY device_health_score ASC
""")

# Temperature trend (hourly)
df_trend = query(conn, f"""
    SELECT
        DATE_TRUNC('hour', event_timestamp)::TIMESTAMP_NTZ AS hour,
        region,
        ROUND(AVG(temperature), 2)   AS avg_temp,
        ROUND(MAX(temperature), 2)   AS max_temp,
        ROUND(MIN(temperature), 2)   AS min_temp
    FROM IOT_SILVER.CLEAN.IOT_EVENTS_CLEAN
    WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '{date_range_days} days'
      {region_filter}
    GROUP BY 1, 2
    ORDER BY 1 DESC
    LIMIT 500
""")

# Recent anomalies
df_anomalies = query(conn, f"""
    SELECT device_id, region, anomaly_type, severity, description, event_timestamp
    FROM IOT_GOLD.KPI.IOT_ANOMALIES
    WHERE event_date >= CURRENT_DATE - 1
      {region_filter}
    ORDER BY event_timestamp DESC
    LIMIT 50
""")

# Data quality stats
df_dq = query(conn, """
    SELECT
        COUNT(*)                           AS total_quarantined,
        rejection_reason,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
    FROM IOT_SILVER.CLEAN.IOT_QUARANTINE
    WHERE quarantined_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
    GROUP BY rejection_reason
    ORDER BY total_quarantined DESC
""")

# ─────────────────────────────────────────────────────────────────────────────
# KPI Cards — Row 1
# ─────────────────────────────────────────────────────────────────────────────
if not df_kpi.empty:
    r = df_kpi.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    cards = [
        (c1, "📟 Total Devices",        f"{int(r.get('total_devices', 0)):,}",    "unique devices"),
        (c2, "📨 Total Events",          f"{int(r.get('total_events', 0)):,}",     f"last {date_range_days}d"),
        (c3, "⬆️ Avg Uptime",           f"{r.get('avg_uptime', 0):.1f}%",         "online availability"),
        (c4, "🌡️ Avg Temperature",      f"{r.get('avg_temperature', 0):.1f}°C",   "all regions"),
        (c5, "🔋 Critical Battery",     f"{int(r.get('critical_battery_devices', 0))}",  "devices <10%"),
        (c6, "🔴 Offline Events",        f"{int(r.get('offline_events', 0))}",     "disconnections"),
    ]
    for col, title, value, label in cards:
        col.markdown(f"""
        <div class="metric-card">
            <div style="font-size:1.2rem">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 2: Map + Temperature Trend
# ─────────────────────────────────────────────────────────────────────────────
col_map, col_trend = st.columns([1.5, 1])

with col_map:
    st.markdown("### 🗺️ Live Device Map")

    # Enrich devices with coordinates from Silver (latest position)
    df_map = query(conn, f"""
        SELECT
            s.device_id,
            s.region,
            s.latitude,
            s.longitude,
            s.temperature,
            s.battery,
            s.signal_strength,
            s.is_online
        FROM IOT_SILVER.CLEAN.IOT_EVENTS_CLEAN s
        INNER JOIN (
            SELECT device_id, MAX(event_timestamp) AS latest
            FROM IOT_SILVER.CLEAN.IOT_EVENTS_CLEAN
            GROUP BY device_id
        ) latest ON s.device_id = latest.device_id AND s.event_timestamp = latest.latest
        WHERE s.event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '2 hours'
        {region_filter}
        LIMIT 200
    """)

    if not df_map.empty:
        df_map["color"] = df_map["is_online"].apply(
            lambda x: [0, 210, 255, 200] if x else [239, 68, 68, 200]
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map,
            get_position=["longitude", "latitude"],
            get_color="color",
            get_radius=5000,
            pickable=True,
            auto_highlight=True,
        )
        view = pdk.ViewState(latitude=39.5, longitude=-98.35, zoom=3.5, pitch=30)
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view,
            map_style="mapbox://styles/mapbox/dark-v10",
            tooltip={
                "text": "Device: {device_id}\nRegion: {region}\nTemp: {temperature}°C\nBattery: {battery}%\nOnline: {is_online}"
            },
        ))
    else:
        st.info("No recent device positions available.")

with col_trend:
    st.markdown("### 📈 Temperature Trend")
    if not df_trend.empty:
        fig = px.line(
            df_trend, x="hour", y="avg_temp", color="region",
            title="Hourly Average Temperature by Region",
            labels={"avg_temp": "Temperature (°C)", "hour": "Time"},
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=380,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No temperature data available.")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Row 3: Device Health Table + Anomaly Alerts
# ─────────────────────────────────────────────────────────────────────────────
col_health, col_alerts = st.columns([1.2, 1])

with col_health:
    st.markdown("### 🔋 Device Health Leaderboard (Worst First)")
    if not df_devices.empty:
        def health_badge(row):
            score = row.get("device_health_score", 100)
            if score < 40:   return f'<span class="badge-critical">Critical {score:.0f}</span>'
            elif score < 70: return f'<span class="badge-warning">Warning {score:.0f}</span>'
            else:            return f'<span class="badge-healthy">Healthy {score:.0f}</span>'

        df_display = df_devices.head(15).copy()
        df_display["Health"] = df_display.apply(health_badge, axis=1)
        df_display["Temp (°C)"]    = df_display["avg_temperature"].map("{:.1f}".format)
        df_display["Battery (%)"]  = df_display["min_battery"].map("{:.1f}".format)
        df_display["Signal (dBm)"] = df_display["avg_signal_strength"].map("{:.0f}".format)
        df_display["Uptime (%)"]   = df_display["uptime_pct"].map("{:.1f}".format)

        st.write(
            df_display[["device_id","region","Health","Temp (°C)","Battery (%)","Signal (dBm)","Uptime (%)"]
            ].to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
    else:
        st.info("No device data.")

with col_alerts:
    st.markdown("### 🚨 Recent Anomalies (Last 24h)")
    if not df_anomalies.empty:
        for _, row in df_anomalies.head(20).iterrows():
            css = "alert-critical" if row.get("severity") == "critical" else "alert-warning"
            icon = "🔴" if row.get("severity") == "critical" else "🟡"
            ts = str(row.get("event_timestamp", ""))[:16]
            st.markdown(f"""
            <div class="{css}">
                {icon} <strong>{row.get('device_id','?')}</strong> [{row.get('anomaly_type','?')}]<br>
                <small>{row.get('description','')}</small><br>
                <small style="color:#6b7280">{ts} · {row.get('region','')}</small>
            </div>""", unsafe_allow_html=True)
    else:
        st.success("✅ No anomalies detected in the last 24 hours.")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Row 4: Data Quality Dashboard
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### 🔍 Data Quality (Last 24h)")
col_dq1, col_dq2 = st.columns(2)

with col_dq1:
    if not df_dq.empty:
        fig_dq = px.bar(
            df_dq, x="rejection_reason", y="total_quarantined",
            title="Quarantined Records by Rejection Reason",
            template="plotly_dark",
            color="rejection_reason",
            color_discrete_sequence=px.colors.qualitative.Antique,
        )
        fig_dq.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False, height=320,
        )
        st.plotly_chart(fig_dq, use_container_width=True)
    else:
        st.success("✅ No quarantined records in last 24h!")

with col_dq2:
    if not df_dq.empty:
        fig_pie = px.pie(
            df_dq, values="total_quarantined", names="rejection_reason",
            title="Rejection Breakdown",
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Antique,
        )
        fig_pie.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=320)
        st.plotly_chart(fig_pie, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Data refreshes from Snowflake Gold layer every 30 seconds · IoT Streaming Platform v1.0")

if auto_refresh:
    time.sleep(30)
    st.rerun()
