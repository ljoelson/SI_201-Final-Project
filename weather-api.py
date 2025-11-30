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

if __name__ == "__main__":
    todays_str = datetime.date.today().strftime("%Y-%m-%d")