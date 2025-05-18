import requests
import json
import pandas as pd
import uuid
from datetime import datetime
import base64
import paho.mqtt.client as mqtt
import time
from io import StringIO
import streamlit as st
import os
import pydeck as pdk
import msgpack


def get_fuel_data():
    client_id = 'CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q'
    client_secret = 'SNmuC7nzn3IISVcG'

    b64_credentials = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
    querystring = {"grant_type": "client_credentials"}
    headers = {
        'Authorization': f'Basic {b64_credentials}',
        'Content-Type': 'application/json'
    }

    token_response = requests.get(token_url, headers=headers, params=querystring)
    # if not token_response.ok:
    #     st.error(f"Token error {token_response.status_code}: {token_response.text}")
    #     st.stop()

    access_token = token_response.json()['access_token']

    fuel_headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json; charset=utf-8',
        'apikey': client_id,
        'transactionid': str(uuid.uuid4()),
        'requesttimestamp': datetime.utcnow().strftime('%d/%m/%Y %I:%M:%S %p')
    }

    url = 'https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices'
    response = requests.get(url, headers=fuel_headers)
    # if not response.ok:
    #     st.error(f"Data error {response.status_code}: {response.text}")
    #     st.stop()

    data = response.json()
    stations = pd.json_normalize(data.get('stations', []))
    prices = pd.json_normalize(data.get('prices', []))
    return stations, prices

def publish_mqtt(client, topic, loaddata, broker="broker.hivemq.com", port=1883):
    
    client.connect(broker, port, 60)
    client.loop_start()
    # print(type(loaddata))
    
    if isinstance(loaddata, list):
        start = time.time()
        info = client.publish(topic, json.dumps(loaddata), qos=1)
        info.wait_for_publish()
        end = time.time()
        print("publish list successful")
        print(f"Publish with confirmation took {end - start:.4f} seconds")
    elif isinstance(loaddata, pd.DataFrame):
        csv_str = loaddata.to_csv(index=False)
        start = time.time()
        info = client.publish(topic, csv_str, qos=1)
        info.wait_for_publish()
        end = time.time()
        print("publish dataframe successful")
        print(f"Publish with confirmation took {end - start:.4f} seconds")
    #     
    
    # client.disconnect()

def clean_data(stations, prices,csv_path="fresh_prices.csv"):
    # print("aaaaa")
    stations = stations.rename(columns={
        'code': 'stationcode',
        'name': 'station_name',
        'location.latitude': 'lat',
        'location.longitude': 'lon'
    }).dropna(subset=['lat', 'lon'])

    stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
    stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')

    prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
    prices = prices[prices['price'] > 0]

    latest_price = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()
    latest_price['record_id'] = (
        latest_price['stationcode'].astype(str) + '_' +
        latest_price['fueltype'].astype(str) + '_' +
        latest_price['lastupdated'].astype(str)
    )
    fuel_options = sorted(latest_price['fueltype'].unique().tolist())
    print(type(fuel_options))
    # print(latest.columns)
    # publish_mqtt("fuel/stations", stations.to_dict(orient="records"))
    # publish_mqtt("fuel/options", fuel_options)

    if os.path.exists(csv_path):
        existing_data = pd.read_csv(csv_path)
        existing_data['record_id'] = (
            existing_data['stationcode'].astype(str) + '_' +
            existing_data['fueltype'].astype(str) + '_' +
            existing_data['lastupdated'].astype(str)
        )
        existing_ids = set(existing_data['record_id'])
    else:
        existing_ids = set()
    # print("current existing data: ",len(existing_data))
   
    new_rows = latest_price[~latest_price['record_id'].isin(existing_ids)].copy()
    new_rows.drop(columns=['record_id'], inplace=True)

    # if len(new_rows)==0:
    #     new_rows="no new price"

    # update new_price data to cvs
    if not new_rows.empty:
        new_rows.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
        print(f"✅ update {len(new_rows)} new prices to {csv_path}")
    else:
        print("ℹ️ no update price")
    print(type(stations))

    if len(existing_data):
        return stations, new_rows, fuel_options, existing_data
    else:
        return stations, new_rows, fuel_options



if __name__=="__main__":
    def push_data():
        stations_raw, prices_raw = get_fuel_data()
        stations,new_prices, fuel_options, price_records = clean_data(stations_raw, prices_raw)
        print(len(new_prices))
        print(type(new_prices))
        client = mqtt.Client()
        publish_mqtt(client,"fuel/stations", stations)
        publish_mqtt(client,"fuel/fuel_options", fuel_options)
        publish_mqtt(client,"fuel/new_prices", new_prices)
        publish_mqtt(client,"fuel/price_records", price_records)
        for record in price_records:
            payload = msgpack.packb(record)  
            client.publish("fuel/price_records", payload, qos=0)
            time.sleep(0.1)
        client.loop_stop()
        client.disconnect()

    while True:
        push_data()
        time.sleep(60)


