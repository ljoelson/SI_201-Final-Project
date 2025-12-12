import requests
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
weatherapi_key = os.getenv("API_KEY")
DB_NAME = "project_data.db"


def get_weather_data(city_name):

    # print("debug api key:", weatherapi_key)

    # detroit
    lat, lon = 42.3314, -83.0458

    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={lat}&lon={lon}&appid={weatherapi_key}"
    )

    try:
        print(f"Fetching real-time weather data for {city_name}...")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()

            if "list" not in data:
                print("Error: API did not include 'list'. Full response:")
                print(data)
                return []

            weather_list = []
            fetch_timestamp = datetime.now().isoformat()

            for entry in data["list"][:25]:
                main = entry["main"]
                weather = entry["weather"][0]
                wind = entry["wind"]

                weather_list.append({
                    "fetch_timestamp": fetch_timestamp,
                    "datetime": entry["dt"], # Unix timestamp
                    "temp": main["temp"],
                    "humidity": main["humidity"],
                    "wind_speed": wind["speed"],
                    "description": weather["description"]
                })

            print(f"Collected {len(weather_list)} forecast rows")
            return weather_list
    
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []

    except Exception as e:
        print(f"Error: {e}")
        return []

def store_weather_data(conn, weather_list):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_timestamp TEXT,
            datetime INTEGER,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT,
            UNIQUE(datetime)
        )
    """)

    inserted = 0 
    skipped = 0

    for w in weather_list:
        try:
            cur.execute("""INSERT INTO WeatherData (datetime, temp, humidity, wind_speed, description)
                VALUES (?, ?, ?, ?, ?)
            """, (w["datetime"], w["temp"], w["humidity"], w["wind_speed"], w["description"]))

            inserted += 1

        except sqlite3.IntegrityError:
            skipped += 1
            

    conn.commit()
    print(f"Weather data successfully stored")
    print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")



if __name__ == "__main__":
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS WeatherData")
    conn.commit()

    weather_data = get_weather_data("Detroit")

    if not weather_data:
        print()
        print("No weather data found")
    else:
        store_weather_data(conn, weather_data)
        
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM WeatherData")
        total = cur.fetchone()[0]
        
        print(f"Total weather records in database: {total}")
        
        if total < 100:
            print(f"   Need {100 - total} more to reach 100. Run script again")
        else:
            print(f"   100 reached")
    
    conn.close()