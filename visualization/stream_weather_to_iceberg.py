from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# SparkSession
spark = (
    SparkSession.builder
      .appName("weather-stream")
      .config("spark.jars.packages",
              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
              "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.661")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lake.type", "hadoop")
      .config("spark.sql.catalog.lake.warehouse", "s3a://lake/warehouse")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


      .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.weather")


# Tạo bảng Iceberg nếu chưa có
spark.sql("""
CREATE TABLE IF NOT EXISTS lake.weather_hourly (
  site STRING,
  lat DOUBLE,
  lon DOUBLE,
  event_time TIMESTAMP,
  temperature DOUBLE,
  precipitation DOUBLE,
  humidity DOUBLE,
  wind_speed DOUBLE,
  ingested_at TIMESTAMP,
  source STRING
)
USING ICEBERG
PARTITIONED BY (days(event_time))
""")
# --- Schema dữ liệu Kafka ---
schema = (
    StructType()
    .add("site", StringType())
    .add("lat", DoubleType())
    .add("lon", DoubleType())
    .add("time", StringType())  # sẽ chuyển sang event_time
    .add("temperature", DoubleType())
    .add("precipitation", DoubleType())
    .add("humidity", DoubleType())
    .add("wind_speed", DoubleType())
    .add("ingested_at", StringType())
    .add("source", StringType())
)

# --- Đọc stream từ Kafka ---
df_raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "weather.raw")
         .option("startingOffsets", "latest")
         .load()
)

# --- Parse JSON và chuẩn hóa schema ---
df = (
    df_raw.selectExpr("CAST(value AS STRING) as json")
          .select(from_json(col("json"), schema).alias("r"))
          .select("r.*")
          .withColumn("event_time", to_timestamp(col("time")))
          .withColumn("ingested_at", to_timestamp(col("ingested_at")))
          .drop("time")
          .select(  # ✅ reorder đúng thứ tự của Iceberg table
              "site",
              "lat",
              "lon",
              "event_time",
              "temperature",
              "precipitation",
              "humidity",
              "wind_speed",
              "ingested_at",
              "source"
          )
)

# --- Ghi stream vào bảng Iceberg trên MinIO ---
query = (
    df.writeStream
      .format("iceberg")
      .option("checkpointLocation", "s3a://lake/checkpoints/weather_hourly")
      .outputMode("append")
      .trigger(processingTime="30 seconds")  # ✅ mỗi 30 giây commit 1 batch
      .toTable("lake.weather_hourly")
)

query.awaitTermination()