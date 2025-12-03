# in Docker container
# curl -X POST -H "Content-Type: application/json" -d @register-s3-sink.json kafka-connect:8083/connectors
# python producer.py

# from localhost
BASE_PATH=${1-"."}
curl -X POST -H "Content-Type: application/json" -d @${BASE_PATH}/register-s3-sink.json localhost:8083/connectors
python3 ${BASE_PATH}/producer.py