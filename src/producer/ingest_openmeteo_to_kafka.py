import os, time, json, requests, datetime as dt
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")

# Danh sách các thành phố
SITES = [
    {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542},
    {"name": "Hue", "lat": 16.4637, "lon": 107.5909},
    {"name": "HCM", "lat": 10.8231, "lon": 106.6297},
]

def fetch_weather(lat, lon):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m"
        "&timezone=Asia%2FBangkok"
    )
    return requests.get(url, timeout=20).json()

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("🚀 Starting producer...")
    while True:
        for site in SITES:
            data = fetch_weather(site["lat"], site["lon"])
            for i, t in enumerate(data["hourly"]["time"]):
                record = {
                    "site": site["name"],
                    "lat": site["lat"],
                    "lon": site["lon"],
                    "time": t,
                    "temperature": data["hourly"]["temperature_2m"][i],
                    "precipitation": data["hourly"]["precipitation"][i],
                    "humidity": data["hourly"]["relative_humidity_2m"][i],
                    "wind_speed": data["hourly"]["wind_speed_10m"][i],
                    "ingested_at": dt.datetime.utcnow().isoformat() + "Z",
                    "source": "open-meteo"
                }
                producer.send(TOPIC, record)
        producer.flush()
        print("✅ Batch sent to Kafka. Waiting 30 seconds...")
        time.sleep(30)
