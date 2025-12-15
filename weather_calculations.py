"""calculate_avg_delay_precipitation(db_conn): calculate avg delay during precipitation
Responsible: Karen
Input: SQLite connection
Output (txt): avg departure delay (float) during rainy/snowy weather
"""

from datetime import datetime

def calc_avg_delay_precip(db_conn):
    cur = db_conn.cursor()
    
    # flight ct
    cur.execute("SELECT COUNT(*) FROM Flights")
    flight_count = cur.fetchone()[0]
    print(f"Total flights: {flight_count}")
    
    # FlightDelays
    cur.execute("SELECT COUNT(*) FROM FlightDelays WHERE delay_minutes IS NOT NULL")
    delay_count = cur.fetchone()[0]
    print(f"Flights with delay data: {delay_count}")
    
    # weather
    cur.execute("SELECT COUNT(*) FROM WeatherData")
    weather_count = cur.fetchone()[0]
    print(f"Total weather records: {weather_count}")
    
    # date ranges; need to join w Timestamps table
    cur.execute("""
        SELECT MIN(T.timestamp), MAX(T.timestamp) 
        FROM Flights F
        JOIN Timestamps T ON F.scheduled_departure_id = T.id
        WHERE T.timestamp IS NOT NULL
    """)
    flight_dates = cur.fetchone()
    print(f"Flight dates range: {flight_dates[0]} to {flight_dates[1]}")
    
    cur.execute("""
        SELECT MIN(FT.timestamp), MAX(FT.timestamp) 
        FROM WeatherData W
        JOIN FetchTimestamps FT ON W.fetch_timestamp_id = FT.id
    """)
    weather_dates = cur.fetchone()
    print(f"Weather fetch dates range: {weather_dates[0]} to {weather_dates[1]}")
    
    # check for preci in weather data
    precip = ['rain', 'snow', 'drizzle', 'sleet', 'hail']
    precip_condition = " OR ".join([f"LOWER(WD.description) LIKE '%{term}%'" for term in precip])
    
    cur.execute(f"""
        SELECT COUNT(*) 
        FROM WeatherData W
        JOIN WeatherDescriptions WD ON W.description_id = WD.id
        WHERE {precip_condition}
    """)
    precip_count = cur.fetchone()[0]
    print(f"Weather records with precipitation: {precip_count}\n")
    
    # join flights w weather n match flights to weather forecasts w/in 3 hours of scheduled departure
    flight_weather_sql = """
        SELECT 
            fd.delay_minutes,
            WD.description,
            T.timestamp as flight_time,
            W.datetime as weather_time
        FROM Flights F
        JOIN FlightDelays fd ON F.flight_id = fd.flight_id
        JOIN Timestamps T ON F.scheduled_departure_id = T.id
        CROSS JOIN WeatherData W
        JOIN WeatherDescriptions WD ON W.description_id = WD.id
        WHERE fd.delay_minutes IS NOT NULL
          AND T.timestamp IS NOT NULL
          AND ABS(
              (strftime('%s', T.timestamp) - W.datetime)
          ) <= 10800
    """
    
    cur.execute(flight_weather_sql)
    rows = cur.fetchall()
    
    print(f"Total flight-weather matches within 3 hours: {len(rows)}")
    
    if len(rows) == 0:
        print("Error: No temporal overlap between flights and weather data. Your flight timestamps and weather forecast timestamps don't align, so collect weather data for the same dates as your flights.")
        return None
    
    # # sample matches
    # print(f"\nSample of matched data (first {min(5, len(rows))} rows):")
    # for i, row in enumerate(rows[:5]):
    #     print(f"Delay: {row[0]} min, Weather: {row[1]}")
    #     print(f"Flight: {row[2]}, Weather: {datetime.fromtimestamp(row[3])}")
    
    # cal avg delay during preci weather
    delays = []
    for delay, desc, flight_time, weather_time in rows:
        desc_lower = desc.lower()
        if any(term in desc_lower for term in precip):
            delays.append(delay)

    if len(delays) == 0:
        print(f"\nNo flights found during precipitation weather.")
        print(f"(Out of {len(rows)} flight-weather matches)")
        return None
    else:
        avg_delay = sum(delays) / len(delays)
        print(f"\nResults:")
        print(f"Flights during precipitation: {len(delays)}")
        print(f"Total flight-weather matches: {len(rows)}")
        print(f"Average departure delay during precipitation: {avg_delay:.2f} minutes")
        return avg_delay
    

if __name__ == "__main__":
    import sqlite3
    
    conn = sqlite3.connect("project_data.db")
    calc_avg_delay_precip(conn)
    conn.close()