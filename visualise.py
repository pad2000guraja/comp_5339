import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import base64
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt


from io import StringIO
import paho.mqtt.client as mqtt
import pandas as pd
import json
from collections import defaultdict
from threading import Thread
import streamlit as st
import paho.mqtt.client as mqtt
import time
from streamlit_autorefresh import st_autorefresh
import queue



import threading


def visualize_data(stations, latest, fuel_options):
    selected_fuel = st.selectbox("Select fuel type", ["All"] + fuel_options)
    # st.write(stations)
    # st.write(fuel_options)
    st.write(latest)
    if selected_fuel != "All":
        latest = latest[latest['fueltype'] == selected_fuel]
    st.write('aaaaa')

    price_pivot = latest.pivot(index='stationcode', columns='fueltype', values='price')
    time_pivot = latest.pivot(index='stationcode', columns='fueltype', values='lastupdated')

    df = stations.merge(price_pivot.reset_index(), on='stationcode')
    df = df.merge(time_pivot.reset_index(), on='stationcode', suffixes=('_price', '_time'))

    fuel_cols = price_pivot.columns
    
    def build_fuel_info(row):
        return "<br/>".join([
            f"{fuel}: {row.get(fuel + '_price')} ¢ ({pd.to_datetime(row.get(fuel + '_time')).strftime('%Y-%m-%d %H:%M')})"
            for fuel in fuel_cols
            if pd.notnull(row.get(fuel + '_price')) and pd.notnull(row.get(fuel + '_time'))
        ])

    df['fuel_info'] = df.apply(build_fuel_info, axis=1)
    st.write(df.isna().sum())
    # df = df.replace({pd.NA: None, float("nan"): None})
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position='[lon, lat]',
        get_fill_color='[0, 120, 250, 160]',
        get_radius=100,
        pickable=True
    )
    st.write('bbbbbbb')
    view = pdk.ViewState(
        latitude=-33.8688,
        longitude=151.2093,
        zoom=10
    )

    tooltip = {
        "html": """
        <div style='font-family: Arial; font-size: 13px;'>
            <b>{station_name}</b><br/>
            {address}<br/><br/>
            <b>Fuel Prices:</b><br/>{fuel_info}
        </div>
        """,
        "style": {
            "backgroundColor": "white",
            "color": "black",
            "border": "1px solid #ccc",
            "padding": "5px",
            "borderRadius": "6px",
            "boxShadow": "2px 2px 6px rgba(0,0,0,0.1)"
        }
    }

    st.pydeck_chart(pdk.Deck(
        map_style='mapbox://styles/mapbox/light-v9',
        initial_view_state=view,
        layers=[layer],
        tooltip=tooltip
    ))


def start_mqtt_client(
    broker_host,
    broker_port,
    topics,
    on_message_callback,
    loop_mode='start'  
):

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message_callback
    client.connect(broker_host, broker_port, keepalive=60)

    for topic in topics:
        client.subscribe(topic)

    if loop_mode == 'start':
        client.loop_start()
    elif loop_mode == 'forever':
        client.loop_forever()
    else:
        raise ValueError("loop_mode must be 'start' or 'forever'")

    return client



station = ""
fuel_list = []    
new_prices = ""
old_price= ""

def on_message(client, userdata, msg):
    global station, fuel_list, new_prices
    topic = msg.topic
    loaddata = msg.payload.decode()  
    # with data_lock:
    
    if topic == "fuel/stations":
        station = loaddata
        
    elif topic == "fuel/new_prices":
        new_prices = loaddata

    elif topic == "fuel/fuel_options":
        # print("dddddd")
        # st.session_state.fuel_list = json.loads(loaddata)
        # fuel_list.clear()
        fuel_list=json.loads(loaddata)






if __name__=="__main__":
    mqtt_client = start_mqtt_client(
    # client_id="my_client",
    broker_host="broker.hivemq.com",
    broker_port=1883,
    topics=[
            "fuel/stations", 
            "fuel/fuel_options",
            "fuel/new_prices"
            # "fuel/price_records"
            ],
    on_message_callback=on_message,
    loop_mode='start'
)

    st.title("fuel price data")
    show_fuel_type = st.empty()
    show_station = st.empty()
    show_prices=st.empty()
    error_check=st.empty()

    fuel_type_data=[]
    station_data=pd.DataFrame()
    new_prices_data=pd.DataFrame()
    record_prices_data=pd.DataFrame()
    
    while True:
        try:
            if fuel_list:
                # show_fuel_type.table(fuel_list)
                fuel_type_data=fuel_list
        
            if station:
                station_data=pd.read_csv(StringIO(station))
                # print("Get all stations success, with total: ",len(station_data))
                # status_check.write("Get all stations success, with total: ",len(station_data))
                # show_station.table(station_data.head())


            if len(new_prices)>0:
                new_prices_data=pd.read_csv(StringIO(new_prices))
                # print("Get all new_prices success, with total: ",len(new_prices))
                # status_check.write("Get all stations success, with total: ",len(station_data))
                # show_prices.table(new_prices_data.head())

            # else :
            #     show_prices.write("no new prices update")
                # new_prices_data=new_prices

            # not use record price yet

        except Exception as e:
            error_check.write(f"Error: {e}")
            print(f"Error: {e}")
            # error_check.write("Waiting Data...")
        show_fuel_type.table(fuel_type_data)
        show_station.table(station_data.head())
        # show_station.table(station_data.head())

        # visualize_data(stations=station_data, latest=new_prices_data, fuel_options=fuel_type_data)

        time.sleep(2)

