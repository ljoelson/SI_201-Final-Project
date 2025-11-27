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


# create tables for weather (openweather api), flights and delays (aviationstack)
def create_tables(conn):

    # weather table: one row per date/city
    cur = conn.cursor("""
    CREATE TABLE IF NOT EXISTS Weather (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        city TEXT NOT NULL,
        temp_c REAL,
        humidity INTEGER,
        wind_speed REAL,
        description TEXT,
        UNIQUE(date, city, description)
    );
    """)

    # flights table: main flight info; integer primary key = flight_id
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Flights (
        flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT,
        airline TEXT,
        dep_iata TEXT,
        arr_iata TEXT,
        scheduled_dep TEXT,
        scheduled_arr TEXT,
        actual_dep TEXT,
        actual_arr TEXT,
        UNIQUE(flight_number, scheduled_dep, dep_iata, arr_iata)
    );
    """)

    # delays table: refs flights by flight_id
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Delays (
        delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_id INTEGER,
        delay_minutes INTEGER,
        reason TEXT,
        FOREIGN KEY(flight_id) REFERENCES Flights(flight_id)
    );
    """)
    conn.commit()

