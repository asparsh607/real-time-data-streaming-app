from kafka import KafkaConsumer
import mysql.connector
import json

# Define Kafka broker and topic configuration
bootstrap_servers = 'localhost:9092'
topic = 'demo1'  

# Define MySQL database configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'flight_data' #database name used in the project.
}

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Create a MySQL database connection
mysql_connection = mysql.connector.connect(**mysql_config)
cursor = mysql_connection.cursor()

# Function to insert data into MySQL
def insert_data_into_mysql(data):
    try:
        # Extract values from the Kafka message
        flight_data = data
        flight_number = flight_data["flight_number"]
        departure_airport = flight_data["departure_airport"]
        arrival_airport = flight_data["arrival_airport"]
        departure_time = flight_data["departure_time"]
        arrival_time = flight_data["arrival_time"]
        booking_price = flight_data["booking_price"]
        
        # Insert data into MySQL
        insert_query = (
            "INSERT INTO flight_data (flight_number, departure_airport, arrival_airport, departure_time, arrival_time, booking_price) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        cursor.execute(insert_query, (flight_number, departure_airport, arrival_airport, departure_time, arrival_time, booking_price))
        mysql_connection.commit()
        print(f"Inserted data for flight {flight_number} into MySQL")
    except Exception as e:
        print(f"Error inserting data into MySQL: {str(e)}")

# Consume and process Kafka messages
try:
    for msg in consumer:
        insert_data_into_mysql(msg.value)

except KeyboardInterrupt:
    pass

finally:
    # Close the MySQL connection and Kafka consumer
    cursor.close()
    mysql_connection.close()
    consumer.close()
