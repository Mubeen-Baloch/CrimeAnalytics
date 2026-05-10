import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
import yaml
from pymongo import MongoClient


@st.cache_data(ttl=10)
def load_config():
    path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def pg_conn(cfg):
    pg = cfg["postgres"]
    host = "localhost" if pg["host"] == "postgres" and not os.path.exists("/app") else pg["host"]
    return psycopg2.connect(host=host, port=pg["port"], dbname=pg["database"], user=pg["user"], password=pg["password"])


@st.cache_data(ttl=15)
def read_sql(query):
    cfg = load_config()
    with pg_conn(cfg) as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=10)
def read_mongo_alerts():
    cfg = load_config()
    uri = cfg["mongodb"]["uri"]
    if uri == "mongodb://mongo:27017" and not os.path.exists("/app"):
        uri = "mongodb://localhost:27018"
    client = MongoClient(uri, serverSelectionTimeoutMS=1500)
    docs = list(client[cfg["mongodb"]["database"]][cfg["mongodb"]["alerts_collection"]].find({}, {"_id": 0}).sort("alert_timestamp", -1).limit(100))
    return pd.DataFrame(docs)


st.set_page_config(page_title="Crime Analytics", layout="wide")
st.title("Real-Time Crime Analytics")

alerts = read_sql("select district, alert_timestamp, event_count, threshold_value, severity from alerts order by alert_timestamp desc limit 100")
trend = read_sql("select trend_type, period_value, crime_count from crime_trends where trend_type in ('year','month','hour') order by trend_type, period_value")
arrests = read_sql("select group_type, group_value, arrest_rate, crime_count from arrest_rates order by arrest_rate desc limit 20")
hotspots = read_sql("select cluster_id, latitude, longitude, crime_count from hotspots order by cluster_id")

left, right = st.columns([1, 1])
with left:
    st.subheader("Latest Alerts")
    st.dataframe(alerts, use_container_width=True, hide_index=True)
with right:
    st.subheader("Top Arrest Rates")
    st.dataframe(arrests, use_container_width=True, hide_index=True)

st.subheader("Crime Trends")
if not trend.empty:
    tab_year, tab_month, tab_hour = st.tabs(["Year", "Month", "Hour"])
    for tab, key in [(tab_year, "year"), (tab_month, "month"), (tab_hour, "hour")]:
        with tab:
            data = trend[trend["trend_type"] == key]
            st.plotly_chart(px.bar(data, x="period_value", y="crime_count"), use_container_width=True)

st.subheader("Hotspot Centroids")
if not hotspots.empty:
    st.map(hotspots.rename(columns={"latitude": "lat", "longitude": "lon"}), latitude="lat", longitude="lon", size="crime_count")

with st.expander("MongoDB alert documents"):
    st.dataframe(read_mongo_alerts(), use_container_width=True, hide_index=True)
