import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml

"""
Batch Processing: CSV to MongoDB Demo
--------------------------------------
Đọc dữ liệu thời tiết từ CSV file, xử lý batch aggregation, ghi vào MongoDB
Batch mode: One-time execution (chạy 1 lần duy nhất khi execute script)
Không có delay - Script kết thúc sau khi hoàn thành, chạy nhiều lần để thành batch insert
Processing: 
  1. Đọc toàn bộ CSV 
  2. Tính aggregation (yearly, monthly, hottest days)
  3. Ghi kết quả vào MongoDB (overwrite), đổi thành append cx đc
"""


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.yaml")
CSV_FILE = os.path.join(SCRIPT_DIR, "weather_history_2000-01-01_2025-12-05.csv")

with open(CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)

MONGODB_URI = config['mongodb']['uri']
MONGODB_DATABASE = config['mongodb']['database']

print("="*60)
print("DEMO: Batch Processing Weather Data to MongoDB")
print("="*60)
print(f"[CSV] File: {CSV_FILE}")
print(f"[MONGO] URI: {MONGODB_URI}")
print(f"[DB] Database: {MONGODB_DATABASE}")
print("="*60)

print("\n[SPARK] Creating Spark Session...")
spark = (
    SparkSession.builder
    .appName("DemoWeatherBatchToMongoDB")
    .config("spark.mongodb.connection.uri", MONGODB_URI)
    .config("spark.mongodb.write.connection.uri", MONGODB_URI)
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")  # Chạy local mode
    .getOrCreate()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[OK] Spark Session created successfully!")
ding CSV file...")
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(CSV_FILE)
)

# Chuyển đổi tên cột và kiểu dữ liệu
print("[PROCESS] Processing data...")
    .withColumnRenamed("Temperature (°C)", "temperature") \
    .withColumnRenamed("Precipitation (mm)", "precipitation") \
    .withColumnRenamed("Humidity (%)", "humidity") \
    .withColumnRenamed("Wind Speed (km/h)", "wind_speed") \
    .withColumnRenamed("Weather Code", "weather_code")

# Chuyển đổi datetime
df = df.withColumn("datetime", F.to_timestamp("datetime"))
df = df.withColumn("year", F.year("datetime"))
df = df.withColumn("month", F.month("datetime"))

# Cache để tăng tốc
df.cache()
total_records = df.count()
df.cache()
total_records = df.count()
print(f"[OK] Loaded {total_records:,} records")

# ============================================
# Batch Processing 1: Tính toán theo năm
# ============================================
print("\n[COMPUTE] Computing yearly statistics...")
yearly_stats = df.groupBy("year").agg(
    F.mean("humidity").alias("avg_humidity"),
    F.sum("precipitation").alias("total_precipitation"),
    F.mean("wind_speed").alias("avg_wind_speed"),
    F.count("*").alias("record_count")
).orderBy("year")

yearly_stats.show()

# Ghi vào MongoDB collection: yearly_summary
print("\n[WRITE] Writing yearly statistics to MongoDB...")
try:
    (
        yearly_stats.write
        .format("mongodb")
        .mode(config['mongodb']['write_mode'])  # overwrite hoặc append
        .option("spark.mongodb.database", MONGODB_DATABASE)
        .option("spark.mongodb.collection", "yearly_summary")
        .save()
    )
    print("[OK] Yearly statistics written successfully!")
except Exception as e:
    print(f"[ERROR] Error writing yearly stati

# ============================================
# Batch Processing 2: Tính toán theo tháng
# ============================================
print("\n[COMPUTE] Computing monthly statistics...")
monthly_stats = df.groupBy("year", "month").agg(
    F.mean("temperature").alias("avg_temperature"),
    F.max("temperature").alias("max_temperature"),
    F.mean("wind_speed").alias("avg_wind_speed"),
    F.count("*").alias("record_count")
).orderBy("year", "month")

print(f"Total months: {monthly_stats.count()}")
monthly_stats.show(12)

# Ghi vào MongoDB collection: monthly_summary
print("\n[WRITE] Writing monthly statistics to MongoDB...")
try:
    (
        monthly_stats.write
        .format("mongodb")
        .mode(config['mongodb']['write_mode'])
        .option("spark.mongodb.connection.uri", MONGODB_URI)
        .option("spark.mongodb.database", MONGODB_DATABASE)
        .option("spark.mongodb.collection", "monthly_summary")
    )
    print("[OK] Monthly statistics written successfully!")
except Exception as e:
    print(f"[ERROR] Error writing monthly statistics: {e}")

# ============================================
# Batch Processing 3: Top 10 ngày nóng nhất
# ============================================
print("\n[HOT] Finding top 10 hottest days...")
hottest_days = (
    df.groupBy("date").agg(
        F.max("temperature").alias("max_temp")
    )
    .orderBy(F.desc("max_temp"))
    .limit(10)

# Ghi vào MongoDB collection: hottest_days
print("\n[WRITE] Writing hottest days to MongoDB...")
try:
    (
        hottest_days.write
        .format("mongodb")
        .mode("overwrite")  # Luôn overwrite vì chỉ lấy top 10
        .option("spark.mongodb.connection.uri", MONGODB_URI)
        .option("spark.mongodb.database", MONGODB_DATABASE)
        .option("spark.mongodb.collection", "hottest_days")
        .save()
    )
    print("[OK] Hottest days written successfully!")
    print(f"[ERROR] Error writing hottest days: {e}")

# ============================================
# (Optional) Ghi raw data vào MongoDB
# ========================
if config['mongodb'].get('save_raw_data', False):
    print(f"\n[WRITE] Writing {config['mongodb']['sample_size']} sample records to MongoDB...")
    sample_df = df.limit(config['mongodb']['sample_size'])
    
    try:
        (
            sample_df.write
            .format("mongodb")
            .mode("overwrite")
            .save()
        )
        print("[OK] Sample raw data written successfully!")
    except Exception as e:
        print(f"[ERROR] Error writing raw data: {e}")

# Dọn dẹp
df.unpersist()
spark.stop()

print("\n" + "="*60)
print("[SUCCESS] DEMO COMPLETED SUCCESSFULLY!")
print("="*60)
print(f"\n[SUMMARY] Summary:")
print(f"   - Total records processed: {total_records:,}")
print(f"   - Collections created:")
print(f"     • yearly_summary")
print(f"     • monthly_summary")
if config['mongodb'].get('save_raw_data', False):
    print(f"     • raw_weather_data")
print(f"\n[CHECK] Check MongoDB database: {MONGODB_DATABASE}")
print("="*60)
