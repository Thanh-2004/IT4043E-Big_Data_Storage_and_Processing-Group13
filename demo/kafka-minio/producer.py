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
INTERVAL = float(os.getenv("PRODUCER_SEND_INTERVAL_SECONDS", "1.0"))

def make_event(i: int):
    return {
        "id": f"{int(time.time()*1000)}-{i}",
        "value": random.random(),
        "source": socket.gethostname(),
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

def main():
    print(f"Connecting to Kafka at {KAFKA_SERVERS} topic={TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )

    i = 0
    try:
        while True:
            event = make_event(i)
            producer.send(TOPIC, value=event)
            # attempt to flush occasionally
            if i % 50 == 0:
                producer.flush()
            print(f"Produced: {event['id']}")
            i += 1
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()