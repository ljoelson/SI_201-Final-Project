"""get_weather_data(city, month): gets weather data for Detroit from OpenWeather API given month
Responsible: Karen
Input: city name (str), month (str)
Output: list of weather information (dictionaries with temp, humidity, wind_speed, description)
store_weather_data(db_conn, weather_list): store weather data in SQL database
Responsible: Karen
Input: SQLite connection, list of weather dicts
Output: inserts data into WeatherData table (checks duplicates)
"""

# open sql connection to access column names

import sqlite3
db_filename = "dawgs_project.db"

def sql_conn(db_file=db_filename):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    return conn