import requests
import datetime
import sqlite3

import os
from dotenv import load_dotenv

load_dotenv()
weatherapi_key = os.getenv("API_KEY")

def get_weather_data(city, month):

    # random detroit lat/lon coordinates
    lat, lon = 42.3314, -83.0458

    # build time range for the given month
    start_date = datetime.datetime(2024, month, 1)
    end_date = start_date + datetime.timedelta(days=25)  # ensures <=25 entries per run!!!!

    url = (
        f"https://history.openweathermap.org/data/2.5/history/city?"
        f"lat={lat}&lon={lon}&type=hour&start={int(start_date.timestamp())}"
        f"&end={int(end_date.timestamp())}&appid={weatherapi_key}")

    response = requests.get(url)
    data = response.json()

    weather_list = []

    if "list" not in data:
        print("Error: No weather data returned.")
        return []

    for entry in data["list"]:
        w = entry["weather"][0]["description"]
        main = entry["main"]

        weather_list.append({
            "datetime": entry["dt"],
            "temp": main["temp"],
            "humidity": main["humidity"],
            "wind_speed": entry["wind"]["speed"],
            "description": w})

#    print(f"code collected {len(weather_list)} weather entries")

    return weather_list


def store_weather_data(conn, weather_list):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime INTEGER UNIQUE,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT)""")

    for w in weather_list:
        try:
            cur.execute("""INSERT OR IGNORE INTO WeatherData (datetime, temp, humidity, wind_speed, description)
                VALUES (?, ?, ?, ?, ?)""", (w["datetime"], w["temp"], w["humidity"], w["wind_speed"], w["description"]))
        except Exception as e:
            print("Insert error:", e)

    conn.commit()
    print("Weather data stored successfully!")