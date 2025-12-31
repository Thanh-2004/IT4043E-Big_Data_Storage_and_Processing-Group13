#!/bin/bash

# Kill any existing port-forwards
echo "🧹 Cleaning up old tunnels..."
pkill -f "kubectl port-forward"

echo "🔌 Opening tunnels..."

# 1. Mongo Express Dashboard (8888 -> 8081)
nohup kubectl port-forward service/mongo-express-service 8888:8081 >/dev/null 2>&1 &
echo "   ✅ Mongo Express:  http://localhost:8888"

# 2. Airflow UI (8080 -> 8080)
nohup kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow >/dev/null 2>&1 &
echo "   ✅ Airflow UI:     http://localhost:8080"

# 3. Kafka (9092 -> 9092)
nohup kubectl port-forward service/kafka-service 9092:9092 >/dev/null 2>&1 &
echo "   ✅ Kafka Broker:   localhost:9092"

# 4. Spark Master UI (8081 -> 8080)
# We map to 8081 to avoid conflict with Airflow
nohup kubectl port-forward service/spark-master-svc 8081:8080 >/dev/null 2>&1 &
echo "   ✅ Spark Master:   http://localhost:8081"

# 5. Spark Job UI (4040 -> 4040)
# This connects directly to the Master Pod where the driver runs
MASTER_POD=$(kubectl get pods -l app=spark-master -o jsonpath="{.items[0].metadata.name}")
nohup kubectl port-forward $MASTER_POD 4040:4040 >/dev/null 2>&1 &
echo "   ✅ Spark Job UI:   http://localhost:4040"

echo "---------------------------------------------------"
echo "🌐 Tunnels are running in the background."
echo "To stop them later, run:  pkill -f 'kubectl port-forward'"
echo "---------------------------------------------------"