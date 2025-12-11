import sqlite3

# Single database name for entire project
DB_NAME = "project_data.db"


def calc_avg_delay_precip(db_conn):
    """
    Calculate average delay during precipitation
    
    Responsible: Karen
    Input: SQLite connection
    Output: avg departure delay (float) during rainy/snowy weather
    """
    cur = db_conn.cursor()
    
    precip = ['rain', 'snow', 'drizzle', 'sleet', 'hail']
    
    # Join flight + weather by closest timestamp before flight
    # This performs a database join as required by project guidelines
    flight_weather_sql = """
        SELECT F.departure_delay, W.description, F.flight_number, F.scheduled_departure
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

    # Check if any precip word in weather descriptions
    delays = []
    precip_flights = []
    
    for delay, desc, flight_num, sched_dep in rows:
        if desc is None:
            continue
        desc_lower = desc.lower()
        if any(term in desc_lower for term in precip):
            if delay is not None:
                delays.append(delay)
                precip_flights.append({
                    'flight_number': flight_num,
                    'scheduled_departure': sched_dep,
                    'delay': delay,
                    'weather': desc
                })

    if len(delays) == 0:
        print("No precipitation-related flights found.")
        return None
    else:
        avg_delay = sum(delays) / len(delays)
        print(f"\n{'='*60}")
        print(f"PRECIPITATION DELAY ANALYSIS")
        print(f"{'='*60}")
        print(f"Total flights with precipitation: {len(delays)}")
        print(f"Average departure delay during precipitation: {avg_delay:.2f} minutes")
        print(f"{'='*60}\n")
        
        # Write results to file as required by project guidelines
        with open("weather_delay_analysis.txt", "w") as f:
            f.write("Weather Delay Analysis\n")
            f.write("="*60 + "\n\n")
            f.write(f"Total flights analyzed with precipitation: {len(delays)}\n")
            f.write(f"Average departure delay during precipitation: {avg_delay:.2f} minutes\n\n")
            f.write("Detailed Flight Data:\n")
            f.write("-"*60 + "\n")
            
            for flight in precip_flights[:10]:  # Show first 10 as sample
                f.write(f"Flight: {flight['flight_number']}\n")
                f.write(f"  Scheduled: {flight['scheduled_departure']}\n")
                f.write(f"  Delay: {flight['delay']} minutes\n")
                f.write(f"  Weather: {flight['weather']}\n\n")
            
            if len(precip_flights) > 10:
                f.write(f"... and {len(precip_flights) - 10} more flights\n")
        
        print("✓ Results written to weather_delay_analysis.txt")

    return avg_delay


def calculate_avg_delay_by_hour(db_conn):
    """
    Additional calculation: Average delay by hour of day
    Shows which times of day have worst delays
    """
    cur = db_conn.cursor()
    
    sql = """
        SELECT 
            CAST(substr(scheduled_departure, 12, 2) AS INTEGER) as hour,
            AVG(departure_delay) as avg_delay,
            COUNT(*) as flight_count
        FROM Flights
        WHERE departure_delay IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """
    
    cur.execute(sql)
    results = cur.fetchall()
    
    print(f"\n{'='*60}")
    print(f"AVERAGE DELAY BY HOUR OF DAY")
    print(f"{'='*60}")
    
    with open("delay_by_hour_analysis.txt", "w") as f:
        f.write("Average Delay by Hour of Day\n")
        f.write("="*60 + "\n\n")
        f.write(f"{'Hour':<10} {'Avg Delay (min)':<20} {'Flight Count':<15}\n")
        f.write("-"*60 + "\n")
        
        for hour, avg_delay, count in results:
            line = f"{hour:02d}:00     {avg_delay:>8.2f}              {count:>5}\n"
            f.write(line)
            print(line.strip())
    
    print(f"{'='*60}\n")
    print("✓ Results written to delay_by_hour_analysis.txt")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("FLIGHT DELAY ANALYSIS")
    print("="*60 + "\n")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        
        # Check if we have data
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM Flights")
        flight_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM WeatherData")
        weather_count = cur.fetchone()[0]
        
        print(f"Database contains:")
        print(f"  - {flight_count} flights")
        print(f"  - {weather_count} weather records\n")
        
        if flight_count < 100:
            print(f"⚠️  Warning: Only {flight_count} flights in database.")
            print(f"   Need at least 100. Run flights_api.py more times.\n")
        
        if weather_count < 100:
            print(f"⚠️  Warning: Only {weather_count} weather records in database.")
            print(f"   Need at least 100. Run weather_api.py more times.\n")
        
        # Perform calculations
        if flight_count > 0 and weather_count > 0:
            calc_avg_delay_precip(conn)
            calculate_avg_delay_by_hour(conn)
        else:
            print("❌ Not enough data to perform analysis.")
            print("   Run flights_api.py and weather_api.py first.\n")
        
        conn.close()
        
    except sqlite3.OperationalError as e:
        print(f"❌ Database error: {e}")
        print("   Make sure you've run flights_api.py and weather_api.py first!\n")
    
    print("="*60)