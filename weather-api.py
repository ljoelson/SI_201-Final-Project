import os
from dotenv import load_dotenv

import requests
import sqlite3
import datetime
import time

load_dotenv()
weatherapi_key = os.getenv("API_KEY")
CITY = "Detroit"
LAT = 42.3314
LON = -83.0458
max_rows = 25

def get_sql_connection():
    conn = sqlite3.connect("dawgs_project.db")
    return conn

def create_weather_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Weather (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            city TEXT,
            temp_c REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT,
            UNIQUE(date, city, description)
        )
    """)
    conn.commit()

def get_weather_for_day(year, month, day):
    dt = datetime.datetime(year, month, day, 12, 0, 0) # 12 0 0 = unix timestamp
    unix_time = int(dt.timestamp())

    url = "https://api.openweathermap.org/data/2.5/onecall/timemachine"
    params = {
        "lat": LAT,
        "lon": LON,
        "dt": unix_time,
        "appid": weatherapi_key,
        "units": "metric"}
    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Error:", response.text[:200])
        return None

    return response.json()

def summarize_weather(json_data): # api request function returns a json object
    if json_data is None:
        return None

    hourly = json_data.get("hourly", [])
    if len(hourly) == 0:
        return None

    temps = []
    humidities = []
    winds = []
    descriptions = []

    for h in hourly:
        temps.append(h.get("temp"))
        humidities.append(h.get("humidity"))
        winds.append(h.get("wind_speed"))

        weather_list = h.get("weather", [])
        if len(weather_list) > 0:
            descriptions.append(weather_list[0].get("description"))

    # summarizing calculations
    temp_avg = sum(temps) / len(temps)
    hum_avg = sum(humidities) / len(humidities)
    wind_avg = sum(winds) / len(winds)

    # most common weather description
    most_common_desc = None
    highest_count = 0

    for d in descriptions:
        count_of_d = descriptions.count(d)

        if count_of_d > highest_count:
            highest_count = count_of_d
            most_common_desc = d

    timestamp = hourly[0]["dt"]

    return date_str, temp_avg, hum_avg, wind_avg, most_common_desc

if __name__ == "__main__":
    todays_str = datetime.date.today().strftime("%Y-%m-%d")
