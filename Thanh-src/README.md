# 0. Preparation
## docker-compose.yml: bản thiết kế system bằng docker
format:
Phần 1: version (1 dòng)
Phần 2: services (danh sách các container trong hệ thống)
  - image (bắt buộc): confluentinc/cp-zookeeper:7.6.1 [Tên Docker org/Tên image:Tên version]
  - ports: "9000:9000" [localhost:9000 -> container:9000]
  - volumes (nếu có dữ liệu): ./minio-data:/data [disk -> container]
  - environment: theo từng service
  - depends_on: đợi A chạy trước rồi B mới được chạy
  - networks: nếu cần kết nối
Phần 3: volumes (lưu dữ liệu ra, nếu ko khi ngắt container thì dữ liệu mất sạch)
Phần 4: networks (nếu ko thì mọi api gọi = Internet, nếu có thì chung network ms thấy nhau, vde bảo mật)

cấu trúc:


# 1. Start Kafka & MinIO
CLI1: start "C:\Program Files\Docker\Docker\Docker Desktop.exe"
CLI1 usage: chạy Docker Desktop, docker service, dockerd, wsl
CLI1 purpose: Khởi động

CLI2: docker compose up -d
CLI2 usage: đọc file docker-compose.yml


# 2. Tạo bucket lake trong MinIO Console

# 3. Start data producer: open cong ket noi cho 2 luong
python ingest_openmeteo_to_kafka.py
docker exec -it thanh-src-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list


# 4. Start Spark streaming consumer: stream


spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.7.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.661 `
  stream_weather_to_iceberg.py
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.7.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.661 `
  stream_weather_to_iceberg.py
# 5. Mở Spark SQL để truy vấn dữ liệu
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.661


# Stack
JDK 11 or 21 :v
Spark 3.5.1
Iceberg 1.6.1 (spark-runtime-3.5_2.12)
Hadoop AWS 3.3.4

open meteo backfill: bthg goi tu api, data -> file lon luon