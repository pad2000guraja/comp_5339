import requests
import base64
import uuid
import time
import json
from datetime import datetime
import pandas as pd
import paho.mqtt.publish as publish

# ---------- OAuth Token ----------
CLIENT_ID = 'CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q'
CLIENT_SECRET = 'SNmuC7nzn3IISVcG'
BROKER = '172.17.34.107'
TOPIC = 'fuel/fuel_data'

b64_credentials = base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
headers = {
    'Authorization': f'Basic {b64_credentials}',
    'Content-Type': 'application/json'
}
token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
token_response = requests.get(token_url, headers=headers, params={"grant_type": "client_credentials"})
access_token = token_response.json()['access_token']

# ---------- Fetch Combined Station + Price Data ----------
fuel_headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json; charset=utf-8',
    'apikey': CLIENT_ID,
    'transactionid': str(uuid.uuid4()),
    'requesttimestamp': datetime.utcnow().strftime('%d/%m/%Y %I:%M:%S %p')
}
url = 'https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices'

while True:
    try:
        response = requests.get(url, headers=fuel_headers)
        data = response.json()

        stations = pd.json_normalize(data.get('stations', []))
        prices = pd.json_normalize(data.get('prices', []))

        if stations.empty or prices.empty:
            print("⚠️ No stations or prices found.")
            time.sleep(60)
            continue

        stations = stations.rename(columns={
            'code': 'stationcode',
            'name': 'station_name',
            'location.latitude': 'lat',
            'location.longitude': 'lon'
        })
        stations = stations.dropna(subset=['lat', 'lon'])
        stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
        stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')

        prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
        prices = prices[prices['price'] > 0]

        latest = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()

        # Build price map
        price_map = {}
        for _, row in latest.iterrows():
            stationcode = row['stationcode']
            if stationcode not in price_map:
                price_map[stationcode] = []
            price_map[stationcode].append({
                "fuelType": row['fueltype'],
                "price": row['price'],
                "lastUpdated": row['lastupdated']
            })

        published_count = 0

        # Merge into full message per station
        for _, row in stations.iterrows():
            stationcode = row['stationcode']
            prices_for_station = price_map.get(stationcode, [])

            # Skip if no valid prices for this station
            if not prices_for_station:
                continue

            enriched_station = {
                "stationid": row.get("stationid"),
                "brand": row.get("brand"),
                "name": row.get("station_name"),
                "address": row.get("address"),
                "location": {
                    "latitude": row.get("lat"),
                    "longitude": row.get("lon")
                },
                "prices": prices_for_station
            }

            publish.single(TOPIC, json.dumps(enriched_station), hostname=BROKER)
            published_count += 1
            time.sleep(0.1)

        print(f"Published {published_count} stations.")
        time.sleep(60)

    except Exception as e:
        print("Error:", e)
        time.sleep(60)
