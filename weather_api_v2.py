import requests
import sqlite3
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
weatherapi_key = os.getenv("API_KEY")

# access data for most recently stored timestamp
def get_last_timestamp(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime INTEGER UNIQUE,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT
        )
    """)
    cur.execute("SELECT MAX(datetime) FROM WeatherData")
    row = cur.fetchone()
    return row[0]

def get_weather_data(city, conn):

    print("fetching weather data")
    print("debug api key:", weatherapi_key)

    url = (
        "https://api.openweathermap.org/data/2.5/forecast"
        f"?q={city}&appid={weatherapi_key}"
    )

    response = requests.get(url)
    data = response.json()

    print("DEBUG top-level keys:", data.keys())

    if "list" not in data:
        print("Error: API did not include 'list'. Full response:")
        print(data)
        return []

    weather_list = []

    for entry in data["list"]:
        main = entry["main"]
        wind = entry.get("wind", {})
        weather_desc = entry["weather"][0]["description"]

        weather_list.append({
            "datetime": entry["dt"], # UNIX timestamp
            "temp": main["temp"],
            "humidity": main["humidity"],
            "wind_speed": wind.get("speed"),
            "description": weather_desc
        })

    print(f"Collected {len(weather_list)} forecast rows.")
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