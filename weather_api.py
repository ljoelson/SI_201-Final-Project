import requests
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()
weatherapi_key = os.getenv("API_KEY")

def get_weather_data(city_name, conn):

    # print("fetching weather data")
    # print("debug api key:", weatherapi_key)

    # random detroit coordinates
    lat, lon = 42.3314, -83.0458

    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={lat}&lon={lon}&appid={weatherapi_key}"
    )

    response = requests.get(url)
    data = response.json()

    print("DEBUG top-level keys:", data.keys())

    if "list" not in data:
        print("Error: API did not include 'list'. Full response:")
        print(data)
        return []

    weather_list = []

    for entry in data["list"][:25]:
        main = entry["main"]
        weather = entry["weather"][0]
        wind = entry["wind"]

        weather_list.append({
            "datetime": entry["dt"], # Unix timestamp
            "temp": main["temp"],
            "humidity": main["humidity"],
            "wind_speed": wind["speed"],
            "description": weather["description"]
        })

    print(f"Collected {len(weather_list)} forecast rows.")
    return weather_list


def store_weather_data(conn, weather_list):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime INTEGER,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT
        )
    """)

    for w in weather_list:
        try:
            cur.execute("""INSERT OR IGNORE INTO WeatherData (datetime, temp, humidity, wind_speed, description)
                VALUES (?, ?, ?, ?, ?)""", (w["datetime"], w["temp"], w["humidity"], w["wind_speed"], w["description"]))
        except Exception as e:
            print("Insert error:", e)

    conn.commit()
    print("Weather data stored successfully!")