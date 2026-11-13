"""Streamlit dashboard for persisted live Frankfurt airspace observations."""
import json
import sys
from pathlib import Path

# When Streamlit launches this file by path (``streamlit run src/dashboard.py``),
# sys.path may contain ``src/`` rather than the repository root. Add the root so
# package imports work regardless of the launcher and current working directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pydeck as pdk
import streamlit as st

from src.repository import Repository

st.set_page_config(page_title="Frankfurt Flight Monitor", layout="wide")
st.title("Frankfurt Airport · Live Airspace Monitor")
st.caption("OpenSky surveillance observations and rule-based analytics. Alerts are informational estimates, not operational instructions.")
st.subheader("Business objectives")
objective_columns = st.columns(3)
objective_columns[0].markdown("**1. Monitor aircraft activity**\n\nShow current aircraft positions and observations around Frankfurt (EDDF).")
objective_columns[1].markdown("**2. Identify possible congestion**\n\nCount observed aircraft in geographic grid zones and flag zones that cross the configured threshold.")
objective_columns[2].markdown("**3. Surface unusual observations**\n\nShow simple rule-based signals such as low airborne speed, large altitude changes, and abrupt heading changes for review.")
st.caption("These are monitoring objectives. The system does not provide official delay data or air traffic control instructions.")
repo = Repository()
aircraft = repo.rows("observations", 2000)
alerts = repo.rows("alerts", 100)
analytics = repo.rows("analytics", 1000)
repo.close()
events = [json.loads(row["payload"]) for row in aircraft]
if not events:
    st.info("No processed aircraft data yet. Start Kafka, then run the consumer and live producer.")
else:
    frame = pd.DataFrame(events)
    latest = frame.sort_values("ingestion_timestamp").drop_duplicates("icao24", keep="last")
    c1, c2, c3 = st.columns(3)
    c1.metric("Aircraft observed", len(latest))
    c2.metric("Observations stored", len(frame))
    c3.metric("Alerts stored", len(alerts))
    st.subheader("Latest aircraft positions")
    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(latitude=50.0379, longitude=8.5622, zoom=8, pitch=0),
        layers=[pdk.Layer("ScatterplotLayer", data=latest, get_position="[longitude, latitude]",
                          get_radius=350, get_fill_color=[20, 120, 220, 180], pickable=True)],
        tooltip={"text": "ICAO: {icao24}\nCallsign: {callsign}\nAltitude: {baro_altitude_m} m"},
    ))
    st.subheader("Recent observations")
    st.dataframe(frame[[c for c in ["icao24", "callsign", "latitude", "longitude", "baro_altitude_m", "velocity_mps", "on_ground", "ingestion_timestamp"] if c in frame]], use_container_width=True)
if analytics:
    st.subheader("Recent zone density")
    density = pd.DataFrame([json.loads(row["payload"]) for row in analytics])
    st.bar_chart(density.groupby("zone_id")["aircraft_count"].max().sort_values(ascending=False).head(15))
if alerts:
    st.subheader("Recent alerts")
    alert_frame = pd.DataFrame([json.loads(row["payload"]) for row in alerts])
    st.dataframe(alert_frame, use_container_width=True)
