import requests
import os
from dotenv import load_dotenv
import sqlite3
from datetime import datetime

load_dotenv()

# Single database name for entire project
DB_NAME = "project_data.db"


def get_flight_data(airport, month=None):
    """
    Retrieve departure data from AviationStack API
    
    Input: 
        airport (str): IATA airport code (e.g., 'DTW', 'JFK', 'LAX')
        month (str): Optional month filter in format 'YYYY-MM' 
    
    Output: 
        list of flight records (max 25 per call)
    """
    
    api_key = os.getenv('AVIATIONSTACK_API_KEY')
    
    if not api_key:
        raise ValueError("API key not found in .env file")
    
    base_url = "http://api.aviationstack.com/v1/flights"
    flights_list = []
    
    params = {
        'access_key': api_key,
        'dep_iata': airport,
        'limit': 25,  # Max 25 items per run
    }
    
    try:
        print(f"Fetching flight data for {airport}...")
        response = requests.get(base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'error' in data:
                print(f"API Error: {data['error']}")
                return flights_list
            
            if 'data' in data:
                print(f"API returned {len(data['data'])} flights")
                
                for flight in data['data']:
                    # Get departure info
                    departure = flight.get('departure', {})
                    arrival = flight.get('arrival', {})
                    flight_info = flight.get('flight', {})
                    airline_info = flight.get('airline', {})
                    
                    scheduled_departure = departure.get('scheduled')
                    actual_departure = departure.get('actual')
                    
                    # Skip if no departure time
                    if not scheduled_departure:
                        continue
                    
                    # If month filter is specified, apply it
                    if month:
                        flight_date = scheduled_departure[:7]
                        if flight_date != month:
                            continue
                    
                    # Calculate departure delay in minutes
                    delay_minutes = 0
                    if actual_departure and scheduled_departure:
                        try:
                            sched_dt = datetime.fromisoformat(scheduled_departure.replace('Z', '+00:00'))
                            actual_dt = datetime.fromisoformat(actual_departure.replace('Z', '+00:00'))
                            delay_minutes = int((actual_dt - sched_dt).total_seconds() / 60)
                        except:
                            delay_minutes = departure.get('delay') or 0
                    else:
                        delay_minutes = departure.get('delay') or 0
                    
                    # Extract flight record
                    flight_record = {
                        'flight_number': flight_info.get('iata', 'N/A'),
                        'airline': airline_info.get('name', 'N/A'),
                        'departure_airport': departure.get('iata', 'N/A'),
                        'arrival_airport': arrival.get('iata', 'N/A'),
                        'scheduled_departure': scheduled_departure,
                        'actual_departure': actual_departure,
                        'scheduled_arrival': arrival.get('scheduled'),
                        'actual_arrival': arrival.get('actual'),
                        'flight_status': flight.get('flight_status', 'unknown'),
                        'departure_delay': delay_minutes  # Changed from delay_minutes to departure_delay
                    }
                    
                    flights_list.append(flight_record)
                
                print(f"Collected {len(flights_list)} flights (after filtering)")
            else:
                print("No flight data in API response")
        else:
            print(f"HTTP Error {response.status_code}: {response.text}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    return flights_list