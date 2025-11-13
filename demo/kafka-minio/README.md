<!-- - Download connector and extract to directory: connect-plugins
- docker compose up -d
- curl ... (send s3 sink connector config to kafka connect to register) -->

# Data Ingestion Stage: Producer -> Kafka Topics -> Kafka Connect -> MinIO

## How to run

1. Clone repo and move to this working directory: from root directory of this repo, run
```
cd demo/kafka-minio
```

2. Download the S3 Sink Kafka Connector [here](https://www.confluent.io/hub/confluentinc/kafka-connect-s3) and extract into `connect-plugins` directory. The directory should contain subdirectories e.g. `assets`, `lib`, etc.

3. Build the producer image and run the containers
```
docker compose up -d
```

4. Send a POST request to the Kafka Connect container to register the S3 sink connector
```
curl -X POST -H "Content-Type: application/json" -d @register-s3-sink.json localhost:8083/connectors
```
Depending on your setup, replace `register-s3-sink.json` with the path to the registration config file, and `localhost:8083` to the proper hostname and port to the Kafka Connect container.

5. `docker compose down` to stop the app; add `-v` to clear files in volumes.

## Workflow

1. `producer.py` runs a loop that creates and sends messages to a Kafka topic using the Kafka Python client
2. Messages are received at Kafka brokers and stored in topics
3. Kafka Connect, when set up with the S3 sink connector, will consume messages from the specified topic and streams them to MinIO for storage

## Notes
1. See saved data files in MinIO using the path `localhost:9001`. Log in with default credentials: username: `minioadmin` and password: `minioadmin`.

2. Verify topic and messages saved in Kafka: run
```
kafka-topics --bootstrap-server kafka:9092 --list
```
to see the list of topics, or
```
kafka-console-consumer \
--bootstrap-server kafka:9092 \
--topic <name-of-topic> \
[--from-beginning]
```
to create a Kafka Consumer to check out new messages in a topic. Here the topic name is set to `events`. Add `--from-beginning` to consume all messages.