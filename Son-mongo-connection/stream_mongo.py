import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml
import time

"""
Stream Processing: CSV Stream to MongoDB Demo
--------------------------------------
Đọc dữ liệu thời tiết từ CSV file stream, xử lý real-time aggregation, ghi vào MongoDB
Stream mode: Continuous execution (chạy liên tục, monitor folder cho file mới)
Processing: 
  1. Monitor CSV directory cho file mới
  2. Tính aggregation real-time (windowed aggregations)
  3. Ghi kết quả vào MongoDB (append mode)
  
Để test: Copy file CSV vào input_stream folder, script sẽ tự động xử lý
"""


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.yaml")
INPUT_STREAM_DIR = os.path.join(SCRIPT_DIR, "input_stream")
CHECKPOINT_DIR = os.path.join(SCRIPT_DIR, "checkpoints")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(INPUT_STREAM_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

with open(CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)

MONGODB_URI = config['mongodb']['uri']
MONGODB_DATABASE = config['mongodb']['database']

print("="*60)
print("DEMO: Stream Processing Weather Data to MongoDB")
print("="*60)
print(f"[STREAM] Input Directory: {INPUT_STREAM_DIR}")
print(f"[CHECKPOINT] Directory: {CHECKPOINT_DIR}")
print(f"[MONGO] URI: {MONGODB_URI}")
print(f"[DB] Database: {MONGODB_DATABASE}")
print("="*60)
print("\n[INFO] Waiting for new CSV files in input_stream folder...")
print("[INFO] Copy CSV files to input_stream folder to process")
print("[INFO] Press Ctrl+C to stop streaming")
print("="*60)

print("\n[SPARK] Creating Spark Session...")
spark = (
    SparkSession.builder
    .appName("DemoWeatherStreamToMongoDB")
    .config("spark.mongodb.connection.uri", MONGODB_URI)
    .config("spark.mongodb.write.connection.uri", MONGODB_URI)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .master("local[*]")  # Chạy local mode
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[OK] Spark Session created successfully!")

# Define schema cho CSV stream (phải có schema rõ ràng cho streaming)
schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("Temperature (°C)", DoubleType(), True),
    StructField("Precipitation (mm)", DoubleType(), True),
    StructField("Humidity (%)", DoubleType(), True),
    StructField("Wind Speed (km/h)", DoubleType(), True),
    StructField("Weather Code", IntegerType(), True),
    StructField("date", StringType(), True)
])

print("[STREAM] Starting stream reader...")
stream_df = (
    spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv(INPUT_STREAM_DIR)
)

# Chuyển đổi tên cột và kiểu dữ liệu
print("[PROCESS] Setting up data transformations...")
stream_df = stream_df \
    .withColumnRenamed("Temperature (°C)", "temperature") \
    .withColumnRenamed("Precipitation (mm)", "precipitation") \
    .withColumnRenamed("Humidity (%)", "humidity") \
    .withColumnRenamed("Wind Speed (km/h)", "wind_speed") \
    .withColumnRenamed("Weather Code", "weather_code")

# Chuyển đổi datetime
stream_df = stream_df.withColumn("datetime", F.to_timestamp("datetime"))
stream_df = stream_df.withColumn("year", F.year("datetime"))
stream_df = stream_df.withColumn("month", F.month("datetime"))
stream_df = stream_df.withColumn("processing_time", F.current_timestamp())

# ============================================
# Stream Processing 1: Raw Data Stream
# ============================================
print("\n[STREAM 1] Setting up raw data stream to MongoDB...")

def write_to_mongodb_raw(df, epoch_id):
    """
    ForeachBatch function để ghi raw data vào MongoDB
    """
    try:
        if df.count() > 0:
            (
                df.write
                .format("mongodb")
                .mode("append")
                .option("spark.mongodb.connection.uri", MONGODB_URI)
                .option("spark.mongodb.database", MONGODB_DATABASE)
                .option("spark.mongodb.collection", "stream_raw_data")
                .save()
            )
            print(f"[BATCH {epoch_id}] Written {df.count()} records to stream_raw_data")
    except Exception as e:
        print(f"[ERROR] Batch {epoch_id} failed: {e}")

query_raw = (
    stream_df.writeStream
    .foreachBatch(write_to_mongodb_raw)
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw_data"))
    .trigger(processingTime='10 seconds')  # Process mỗi 10 giây
    .start()
)

# ============================================
# Stream Processing 2: Yearly Aggregation
# ============================================
print("[STREAM 2] Setting up yearly aggregation stream...")

yearly_agg = stream_df.groupBy("year").agg(
    F.mean("temperature").alias("avg_temperature"),
    F.mean("humidity").alias("avg_humidity"),
    F.sum("precipitation").alias("total_precipitation"),
    F.mean("wind_speed").alias("avg_wind_speed"),
    F.count("*").alias("record_count"),
    F.max("processing_time").alias("last_updated")
)

def write_to_mongodb_yearly(df, epoch_id):
    """
    ForeachBatch function để ghi yearly aggregation vào MongoDB
    Update mode: overwrite các documents có cùng year
    """
    try:
        if df.count() > 0:
            # Đọc dữ liệu hiện có từ MongoDB
            # Merge với dữ liệu mới và update
            (
                df.write
                .format("mongodb")
                .mode("append")  # Trong streaming, dùng append và handle duplicates ở MongoDB
                .option("spark.mongodb.connection.uri", MONGODB_URI)
                .option("spark.mongodb.database", MONGODB_DATABASE)
                .option("spark.mongodb.collection", "stream_yearly_summary")
                .save()
            )
            print(f"[BATCH {epoch_id}] Updated yearly aggregations")
    except Exception as e:
        print(f"[ERROR] Yearly batch {epoch_id} failed: {e}")

query_yearly = (
    yearly_agg.writeStream
    .foreachBatch(write_to_mongodb_yearly)
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "yearly_agg"))
    .outputMode("complete")  # Complete mode cho aggregation
    .trigger(processingTime='30 seconds')  # Process mỗi 30 giây
    .start()
)

# ============================================
# Stream Processing 3: Monthly Aggregation
# ============================================
print("[STREAM 3] Setting up monthly aggregation stream...")

monthly_agg = stream_df.groupBy("year", "month").agg(
    F.mean("temperature").alias("avg_temperature"),
    F.max("temperature").alias("max_temperature"),
    F.min("temperature").alias("min_temperature"),
    F.mean("humidity").alias("avg_humidity"),
    F.mean("wind_speed").alias("avg_wind_speed"),
    F.count("*").alias("record_count"),
    F.max("processing_time").alias("last_updated")
)

def write_to_mongodb_monthly(df, epoch_id):
    """
    ForeachBatch function để ghi monthly aggregation vào MongoDB
    """
    try:
        if df.count() > 0:
            (
                df.write
                .format("mongodb")
                .mode("append")
                .option("spark.mongodb.connection.uri", MONGODB_URI)
                .option("spark.mongodb.database", MONGODB_DATABASE)
                .option("spark.mongodb.collection", "stream_monthly_summary")
                .save()
            )
            print(f"[BATCH {epoch_id}] Updated monthly aggregations")
    except Exception as e:
        print(f"[ERROR] Monthly batch {epoch_id} failed: {e}")

query_monthly = (
    monthly_agg.writeStream
    .foreachBatch(write_to_mongodb_monthly)
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "monthly_agg"))
    .outputMode("complete")
    .trigger(processingTime='30 seconds')
    .start()
)

# ============================================
# Stream Processing 4: Windowed Aggregation (Last Hour)
# ============================================
print("[STREAM 4] Setting up windowed aggregation stream...")

# Tumbling window: mỗi 1 giờ
windowed_agg = stream_df \
    .withWatermark("datetime", "2 hours") \
    .groupBy(
        F.window("datetime", "1 hour")
    ).agg(
        F.mean("temperature").alias("avg_temperature"),
        F.max("temperature").alias("max_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.mean("humidity").alias("avg_humidity"),
        F.count("*").alias("record_count"),
        F.max("processing_time").alias("last_updated")
    )

def write_to_mongodb_windowed(df, epoch_id):
    """
    ForeachBatch function để ghi windowed aggregation vào MongoDB
    """
    try:
        if df.count() > 0:
            # Flatten window struct
            df_flat = df.select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "avg_temperature",
                "max_temperature",
                "min_temperature",
                "avg_humidity",
                "record_count",
                "last_updated"
            )
            
            (
                df_flat.write
                .format("mongodb")
                .mode("append")
                .option("spark.mongodb.connection.uri", MONGODB_URI)
                .option("spark.mongodb.database", MONGODB_DATABASE)
                .option("spark.mongodb.collection", "stream_hourly_summary")
                .save()
            )
            print(f"[BATCH {epoch_id}] Updated hourly windowed aggregations")
    except Exception as e:
        print(f"[ERROR] Windowed batch {epoch_id} failed: {e}")

query_windowed = (
    windowed_agg.writeStream
    .foreachBatch(write_to_mongodb_windowed)
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "windowed_agg"))
    .outputMode("append")  # Append mode cho windowed aggregation
    .trigger(processingTime='30 seconds')
    .start()
)

# ============================================
# Monitor Streams
# ============================================
print("\n" + "="*60)
print("[STREAMING] All streams started successfully!")
print("="*60)
print("\n[STATUS] Active Streams:")
print("  1. Raw Data Stream        -> stream_raw_data")
print("  2. Yearly Aggregation     -> stream_yearly_summary")
print("  3. Monthly Aggregation    -> stream_monthly_summary")
print("  4. Hourly Windows         -> stream_hourly_summary")
print("\n[ACTION] Copy CSV files to input_stream folder to process")
print("[ACTION] Press Ctrl+C to stop all streams")
print("="*60)

try:
    # Wait for all streams
    query_raw.awaitTermination()
except KeyboardInterrupt:
    print("\n\n[STOP] Stopping all streams...")
    query_raw.stop()
    query_yearly.stop()
    query_monthly.stop()
    query_windowed.stop()
    spark.stop()
    print("[OK] All streams stopped successfully!")
    print("="*60)
