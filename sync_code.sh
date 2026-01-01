#!/bin/bash

# 1. Get Pod Names
SCHEDULER="airflow-scheduler-0"
WEBSERVER=$(kubectl get pods -n airflow -l component=webserver -o jsonpath="{.items[0].metadata.name}")

if [ -z "$WEBSERVER" ]; then
  echo "❌ Error: Webserver pod not found."
  exit 1
fi

echo "Targeting: $SCHEDULER & $WEBSERVER"

# 2. Create Directories
kubectl exec -n airflow $SCHEDULER -- mkdir -p /opt/airflow/spark_jobs > /dev/null 2>&1
kubectl exec -n airflow $WEBSERVER -- mkdir -p /opt/airflow/spark_jobs > /dev/null 2>&1

# 3. Sync DAGs
kubectl cp src/airflow/dags/weather_dag.py airflow/$SCHEDULER:/opt/airflow/dags/weather_dag.py
kubectl cp src/airflow/dags/weather_dag.py airflow/$WEBSERVER:/opt/airflow/dags/weather_dag.py

# 4. Sync Scripts
kubectl cp src/spark/jobs/weather_etl.py airflow/$SCHEDULER:/opt/airflow/spark_jobs/weather_etl.py
kubectl cp src/spark/jobs/weather_etl.py airflow/$WEBSERVER:/opt/airflow/spark_jobs/weather_etl.py

# 5. Syntax Check
echo "Checking syntax..."
kubectl exec -n airflow $SCHEDULER -- python3 /opt/airflow/dags/weather_dag.py

if [ $? -eq 0 ]; then
  echo "✅ Sync Complete. Syntax Valid."
else
  echo "❌ Syntax Error."
fi