from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import random

# Define Kafka broker and topic
bootstrap_servers = 'localhost:9092'
topic = 'demo1' 

# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Extended list of airport names
airport_names = ["JFK", "LAX", "ORD", "ATL", "DFW", "LHR", "CDG", "HKG", "SYD", "DXB", "SFO", "DEN", "SEA", "MIA", "MSP", "LAS", "PHX", "YYZ", "MEX"]


# Simulate real-time flight data flow
while True:
    
    flight_number = random.randint(1000, 9999)
    
    # Ensure departure and arrival airports are different
    departure_airport = random.choice(airport_names)
    arrival_airport = departure_airport
    
    while arrival_airport == departure_airport:
        arrival_airport = random.choice(airport_names)

    # Generate random departure time within a date range (e.g., 30 days)
    start_date = datetime(2023, 9, 11) # Change to current date if need.
    end_date = start_date + timedelta(days=30)  # Specify the date range
    departure_time = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(1, 12))

    # Generate random arrival time after departure time
    arrival_time = departure_time + timedelta(hours=random.randint(1, 12))
    booking_price = round(random.uniform(100, 1000), 2)

    # Create a flight data point
    flight_data_point = {
        "flight_number": flight_number,
        "departure_airport": departure_airport,
        "arrival_airport": arrival_airport,
        "departure_time": departure_time.strftime("%Y-%m-%d %H:%M"),
        "arrival_time": arrival_time.strftime("%Y-%m-%d %H:%M"),
        "booking_price": booking_price
    }

    producer.send(topic=topic, value=flight_data_point)
    
    # Simulate real-time interval (e.g., 1 second)
    time.sleep(1)
    