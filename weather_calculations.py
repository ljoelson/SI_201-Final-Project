"""calculate_avg_delay_precipitation(db_conn): calculate avg delay during precipitation
Responsible: Karen
Input: SQLite connection
Output (txt): avg departure delay (float) during rainy/snowy weather
"""

def calc_avg_delay_precip(db_conn):
    cur = db_conn.cursor()
    
    precip = ['rain', 'snow', 'drizzle', 'sleet', 'hail']
    
    # Join flight delays with weather data
    # Match flights to weather records from the same fetch_date
    flight_weather_sql = """
        SELECT fd.delay_minutes, W.description
        FROM Flights F
        JOIN flight_delays fd ON F.flight_id = fd.flight_id
        JOIN WeatherData W
          ON DATE(F.scheduled_departure) = W.fetch_date
        WHERE fd.delay_minutes IS NOT NULL
          AND W.description IS NOT NULL
    """

    cur.execute(flight_weather_sql)
    rows = cur.fetchall()
    
    if not rows:
        print("No flight data with weather information found.")
        return None

    # Check if any precip word in weather descriptions
    delays = []
    for delay, desc in rows:
        desc_lower = desc.lower()
        if any(term in desc_lower for term in precip):
            delays.append(delay)

    if len(delays) == 0:
        print("No precipitation-related flights found.")
        return None
    else:
        avg_delay = sum(delays) / len(delays)
        print(f"Average departure delay during precipitation: {avg_delay:.2f} minutes")
        print(f"Total flights during precipitation: {len(delays)}")
        print(avg_delay)
        return avg_delay