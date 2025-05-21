import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import paho.mqtt.client as mqtt
import json
import time

st.set_page_config(layout="wide")
st.title("NSW Fuel Stations - Real-time Dashboard")

latest_data = []

def on_connect(client, userdata, flags, rc):
    st.sidebar.success("Connected to MQTT Broker")
    client.subscribe("fuel/fuel_data")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        latest_data.append(payload)
    except Exception as e:
        st.error(f"Message decoding error: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect("172.17.34.107", 1883, 60)
    client.loop_start()
    st.sidebar.info("Listening for fuel data via MQTT...")
except Exception as e:
    st.sidebar.error(f"Connection failed: {e}")

with st.spinner("Waiting for data..."):
    time.sleep(5)

client.loop_stop()

if not latest_data:
    st.warning("No data received yet.")
else:
    # Collect all available fuel types
    fuel_types = set()
    for station in latest_data:
        for fuel in station.get("prices", []):
            fuel_types.add(fuel.get("fuelType", "N/A"))

    default_fuel_type = st.selectbox("Select default fuel type to display", sorted(fuel_types))


    st.subheader("Map View")
    st.subheader("Recently Updated Stations")

    # Build a flat table with fuel price info
    rows = []
    for station in latest_data:
        brand = station.get("brand", "Unknown")
        name = station.get("name", "Unnamed")
        address = station.get("address", "No address")
        for fuel in station.get("prices", []):
            rows.append({
                "Brand": brand,
                "Station Name": name,
                "Address": address,
                "Fuel Type": fuel.get("fuelType", "N/A"),
                "Price (¢)": fuel.get("price", "N/A"),
                "Last Updated": fuel.get("lastUpdated", "N/A")
            })

    # Create DataFrame and sort by latest updates
    df_prices = pd.DataFrame(rows)
    if not df_prices.empty:
        df_prices["Last Updated"] = pd.to_datetime(df_prices["Last Updated"], errors="coerce")
        df_prices = df_prices.sort_values("Last Updated", ascending=False)
        st.dataframe(df_prices)
    else:
        st.info("No fuel price updates available.")
    m = folium.Map(location=[-33.8688, 151.2093], zoom_start=10)

    for station in latest_data:
        lat = station.get("location", {}).get("latitude")
        lon = station.get("location", {}).get("longitude")
        if lat is None or lon is None:
            continue

        brand = station.get("brand", "Unknown")
        name = station.get("name", "Unnamed")
        address = station.get("address", "No address")
        fuel_prices = station.get("prices", [])

        default_price = ""
        fuel_lines = ""

        for fuel in fuel_prices:
            fuel_type = fuel.get("fuelType", "N/A")
            price = fuel.get("price", "N/A")
            updated = fuel.get("lastUpdated", "N/A")
            fuel_lines += f"{fuel_type}: {price} ¢ ({updated})<br>"

            if fuel_type == default_fuel_type:
                default_price = f"<div style='background:#1E90FF;color:white;padding:2px 6px;border-radius:4px;margin-bottom:4px;font-weight:bold;'>{price} ¢ • {fuel_type}</div>"

        if not default_price:
            default_price = f"<div style='color:gray;font-style:italic;margin-bottom:4px;'>No data for selected fuel type ({default_fuel_type})</div>"

        popup_html = f"""
        {default_price}
        <b>{brand} - {name}</b><br>
        {address}<br><br>
        <b>Fuel Prices:</b><br>{fuel_lines}
        """

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(m)

    st_folium(m, width=900, height=600)
