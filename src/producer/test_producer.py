import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Serializer to turn Python dictionaries into JSON bytes
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Connect to localhost:9092 (This requires the port-forward to be running!)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

cities = ["London", "New York", "Tokyo", "Hanoi", "Paris"]

print("Starting Weather Producer...")
print("Press Ctrl+C to stop.")

while True:
    # Create a fake weather report
    data = {
        "city": random.choice(cities),
        "temperature": round(random.uniform(10.0, 35.0), 1),
        "timestamp": datetime.now().isoformat()
    }
    
    # Send it to the 'weather_data' topic
    producer.send("weather_data", data)
    print(f"Sent: {data['city']} - {data['temperature']}C")
    
    time.sleep(2)