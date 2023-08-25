#!/usr/bin/env python
# coding: utf-8

# In[4]:


import random
import time
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from kafka import KafkaProducer

# OpenWeatherMap API settings
api_key = "6156d68254dfe8638575e65ff5c9d4f2"
weather_api_url = "http://api.openweathermap.org/data/2.5/weather"
city = "New York"
units = "metric"

# MySQL database settings
db_host = "localhost"
db_user = "root"
db_password = "ni$ha17111998"
db_name = "stock"


# Kafka settings
kafka_broker = "localhost:9092"
kafka_topic = "temperature_readings"
# Simulated thermostat configuration
num_thermostats = 3

def generate_temperature(weather_data):
    base_temperature = 20.0
    temperature_range = 5.0
    weather_temperature = weather_data["main"]["temp"]
    return base_temperature + random.uniform(-temperature_range, temperature_range) + (weather_temperature - 20.0)

def get_weather_data():
    params = {
        "q": city,
        "units": units,
        "appid": api_key
    }
    try:
        response = requests.get(weather_api_url, params=params)
        response.raise_for_status()  # Raise an exception if request fails
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Failed to fetch weather data:", e)
        return None

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker)

# Create MySQL engine
db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(db_url)

# Initialize DataFrame
columns = ["Timestamp", "Thermostat_ID", "Temperature"]
data = []

while True:
    weather_data = get_weather_data()
    if weather_data:
        for thermostat_id in range(1, num_thermostats + 1):
            temperature = generate_temperature(weather_data)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add data to DataFrame
            data.append([timestamp, thermostat_id, temperature])
            
            # Send data to Kafka topic
            kafka_data = f"{timestamp},{thermostat_id},{temperature}"
            producer.send(kafka_topic, value=kafka_data.encode("utf-8"))
            
            # Sleep for a short interval before sending next data
            time.sleep(1)
            
    else:
        print("Failed to fetch weather data")
    
    # Convert data to DataFrame and store in MySQL
    if data:
        df = pd.DataFrame(data, columns=columns)
        df.to_sql("temperature_data", con=engine, if_exists="append", index=False)
        print("Data stored in MySQL and sent to Kafka")
        data = []  # Clear data list
    
    time.sleep(3)  # Fetch data every 5 minutes


# In[ ]:




