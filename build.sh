#!/bin/bash

# ==============================================================================
# 🌦️ WEATHER PIPELINE - AUTOMATED DEPLOYMENT SCRIPT
# ==============================================================================

# Colors for pretty output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Deployment Sequence...${NC}"

# ------------------------------------------------------------------------------
# STEP 1: DEPLOY CORE INFRASTRUCTURE (Kafka, Mongo, Spark)
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[1/6] Applying Kubernetes Manifests...${NC}"
kubectl create namespace mongodb --dry-run=client -o yaml | kubectl apply -f -echo -e "${GREEN}    ✅ Namespace 'mongodb' ensures.${NC}"

kubectl apply -f manifests/
# ------------------------------------------------------------------------------
# STEP 2: DEPLOY AIRFLOW (Helm)
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[2/6] Deploying Airflow...${NC}"

# Ensure Helm repo exists
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1
helm repo update >/dev/null 2>&1

# Install/Upgrade Airflow using your custom values
# Release Name: 'airflow' -> Service Name: 'airflow-webserver'
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f helm/airflow-values.yaml

echo -e "${GREEN}    ✅ Airflow deployment initiated.${NC}"

# ------------------------------------------------------------------------------
# STEP 3: WAIT FOR SPARK MASTER
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[3/6] Waiting for Spark Master to be ready...${NC}"

# Loop until the pod is found and running
while true; do
  MASTER_POD=$(kubectl get pods -l app=spark-master -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)
  
  if [ -n "$MASTER_POD" ]; then
    STATUS=$(kubectl get pod "$MASTER_POD" -o jsonpath="{.status.phase}")
    if [ "$STATUS" == "Running" ]; then
      echo -e "${GREEN}    ✅ Spark Master is Running: $MASTER_POD${NC}"
      break
    fi
  fi
  
  echo -n "."
  sleep 3
done

# Short pause to ensure networking is fully up
sleep 5

# ------------------------------------------------------------------------------
# STEP 4: DEPLOY SPARK CODE
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[4/6] Uploading ETL Script to Cluster...${NC}"

LOCAL_SCRIPT="src/spark/jobs/weather_etl.py"
REMOTE_PATH="/opt/spark/jobs/weather_etl.py"

if [ ! -f "$LOCAL_SCRIPT" ]; then
    echo -e "${RED}❌ Error: File $LOCAL_SCRIPT not found! Check your tree.${NC}"
    exit 1
fi

kubectl cp "$LOCAL_SCRIPT" default/$MASTER_POD:$REMOTE_PATH
echo -e "${GREEN}    ✅ Script uploaded successfully.${NC}"

# ------------------------------------------------------------------------------
# STEP 5: SUBMIT SPARK JOB
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[5/6] Submitting Spark Job...${NC}"

# Run interactively in the background
kubectl exec -it -n default deployment/spark-master -- bash -c 'export POD_IP=$(hostname -i); /opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --conf spark.driver.host=$POD_IP \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /opt/spark/jobs/weather_etl.py' &

SPARK_PID=$!
sleep 5

# ------------------------------------------------------------------------------
# STEP 6: FORWARD PORTS
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[6/6] Establishing Network Tunnels...${NC}"
if [ -x "./ports.sh" ]; then
    ./ports.sh
else
    echo -e "${RED}⚠️  Warning: ports.sh not found or not executable. Make sure to chmod +x ports.sh${NC}"
fi

echo -e "\n${GREEN}🎉 DEPLOYMENT COMPLETE!${NC}"
echo -e "----------------------------------------------------------------"
echo -e "📊 Mongo Express:  http://localhost:8888"
echo -e "🌬️  Airflow UI:     http://localhost:8080 (User/Pass: admin)"
echo -e "🌪️  Spark Job PID:  $SPARK_PID"
echo -e "----------------------------------------------------------------"
echo -e "⚠️  IMPORTANT: Now start the Producer in a NEW terminal:"
echo -e "   ${BLUE}python3 src/producer/test_producer.py${NC}"
echo -e "----------------------------------------------------------------"

wait $SPARK_PID