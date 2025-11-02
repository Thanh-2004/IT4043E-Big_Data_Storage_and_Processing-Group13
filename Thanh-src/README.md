# 1. Start Kafka & MinIO
docker compose up -d


# 2. Tạo bucket lake trong MinIO Console

# 3. Start data producer
python3 ingest_openmeteo_to_kafka.py
docker exec -it thanh-src-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list


# 4. Start Spark streaming consumer
brew install apache-spark

spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.661 \
  stream_weather_to_iceberg.py

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.661 \
  stream_weather_to_iceberg.py


# 5. Mở Spark SQL để truy vấn dữ liệu
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.661


# Stack
JDK 11 or 21 :v
Spark 3.5.1
Iceberg 1.6.1 (spark-runtime-3.5_2.12)
Hadoop AWS 3.3.4
