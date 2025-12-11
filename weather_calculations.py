"""calculate_avg_delay_precipitation(db_conn): calculate avg delay during precipitation
Responsible: Karen
Input: SQLite connection
Output (txt): avg departure delay (float) during rainy/snowy weather
"""

def calc_avg_delay_precip(db_conn):
    cur = db_conn.cursor()
    
    precip = ['rain', 'snow', 'drizzle', 'sleet', 'hail']
    
    # join flight + weather by closest timestamp before flight
    flight_weather_sql = """
        SELECT F.departure_delay, W.description
        FROM Flights F
        JOIN WeatherData W
          ON W.datetime = (
               SELECT MAX(datetime)
               FROM WeatherData
               WHERE datetime <= F.scheduled_departure
          )
    """

    cur.execute(flight_weather_sql)
    rows = cur.fetchall()

    # check if any precip word in weather descrps
    delays = []
    for delay, desc in rows:
        if desc is None:
            continue
        desc_lower = desc.lower()
        if any(term in desc_lower for term in precip):
            if delay is not None:
                delays.append(delay)

    if len(delays) == 0:
        print("No precipitation-related flights found.")
        return None
    else:
        avg_delay = sum(delays) / len(delays)
        print(f"Average departure delay during precipitation: {avg_delay:.2f} minutes")

    return avg_delay