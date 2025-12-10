
import os
import sqlite3
from weather_api_v2 import get_weather_data, store_weather_data

DB_NAME = "final_project.db"

def main():
    print("Using database file:", os.path.abspath(DB_NAME))
    conn = sqlite3.connect(DB_NAME)

    weather_list = get_weather_data("Detroit", conn=conn)
    store_weather_data(conn, weather_list)

    conn.close()
    print("\nAll done!")

if __name__ == "__main__":
    main()