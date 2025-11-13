curl -X POST -H "Content-Type: application/json" -d @register-s3-sink.json kafka-connect:8083/connectors
python producer.py