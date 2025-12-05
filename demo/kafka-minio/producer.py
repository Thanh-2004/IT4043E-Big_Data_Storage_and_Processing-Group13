#!/usr/bin/env python3
"""
Simple Kafka producer that sends JSON messages to topic defined in env.
Run inside container: python producer.py
"""
import os
import json
import time
import random
import socket
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
INTERVAL = float(os.getenv("PRODUCER_SEND_INTERVAL_SECONDS", "30.0"))

SITES = [
    {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542},
    {"name": "Hue", "lat": 16.4637, "lon": 107.5909},
    {"name": "HCM", "lat": 10.8231, "lon": 106.6297},
]

def fetch_weather(lat, lon):
    url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&current=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m"
            "&timezone=Asia%2FBangkok"
        )
    return requests.get(url, timeout=20).json()

# def make_event(i: int):
#     return {
#         "id": f"{int(time.time()*1000)}-{i}",
#         "value": random.random(),
#         "source": socket.gethostname(),
#         "created_at": datetime.utcnow().isoformat() + "Z",
#     }

# def main():
#     print(f"Connecting to Kafka at {KAFKA_SERVERS} topic={TOPIC}")
#     producer = KafkaProducer(
#         bootstrap_servers=[KAFKA_SERVERS],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks='all',
#         retries=3,
#         batch_size=16384,
#         linger_ms=10,
#         compression_type='gzip'
#     )

#     i = 0
#     try:
#         while True:
#             event = make_event(i)
#             producer.send(TOPIC, value=event)
#             # attempt to flush occasionally
#             if i % 50 == 0:
#                 producer.flush()
#             print(f"Produced: {event['id']}")
#             i += 1
#             time.sleep(INTERVAL)
#     except KeyboardInterrupt:
#         print("Producer stopped by user.")
#     finally:
#         producer.flush()
#         producer.close()

def main():
    print(f"Connecting to Kafka at {KAFKA_SERVERS} topic={TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Starting producer...")
    try:
        while True:
            for site in SITES:
                data = fetch_weather(site["lat"], site["lon"])
                current = data.get("current", {})
                
                if not current:
                    print(f"⚠️ No data for {site['name']}")
                    continue

                record = {
                    "site": site["name"],
                    "lat": site["lat"],
                    "lon": site["lon"],
                    # Lấy thời gian thực từ API (hoặc dùng dt.datetime.now())
                    "time": current.get("time"), 
                    "temperature": current.get("temperature_2m"),
                    "precipitation": current.get("precipitation"),
                    "humidity": current.get("relative_humidity_2m"),
                    "wind_speed": current.get("wind_speed_10m"),
                    "ingested_at": dt.datetime.utcnow().isoformat() + "Z",
                    "type": "realtime"
                }
                producer.send(TOPIC, value=record)

            producer.flush()
            print(f"Data sent to Kafka. Waiting {INTERVAL} seconds...")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()