import time
import os
import logging
from pymongo import MongoClient
from prometheus_client import start_http_server, Gauge

# --- CẤU HÌNH ---
# Lưu ý: Collection bây giờ là 'batch_gold' theo file silver_to_gold.py
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/')
DB_NAME = os.getenv('DB_NAME', 'weather_db')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'batch_gold')
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', 15))

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ĐỊNH NGHĨA METRICS PROMETHEUS (GOLD LAYER SCHEMA) ---

# 1. Các chỉ số cơ bản (Physical)
M_TEMP = Gauge('weather_temperature_celsius', 'Nhiệt độ không khí (2m)', ['city'])
M_HUMIDITY = Gauge('weather_humidity_percent', 'Độ ẩm tương đối', ['city'])
M_WIND_SPEED = Gauge('weather_wind_speed_kmh', 'Tốc độ gió (10m)', ['city'])
M_PRECIPITATION = Gauge('weather_precipitation_mm', 'Lượng mưa', ['city'])
M_SOIL_TEMP = Gauge('weather_soil_temperature_celsius', 'Nhiệt độ đất (0-7cm)', ['city'])
M_SOIL_MOISTURE = Gauge('weather_soil_moisture_m3m3', 'Độ ẩm đất', ['city'])

# 2. Các chỉ số nâng cao (Advanced Meteorological Features) - Rất quan trọng cho Dashboard xịn
M_APP_TEMP = Gauge('weather_apparent_temperature_celsius', 'Nhiệt độ cảm nhận thực tế (Apparent Temp)', ['city'])
M_VPD = Gauge('weather_vapor_pressure_deficit_kpa', 'Thâm hụt áp suất hơi nước (Chỉ số khô hạn)', ['city'])
M_DEW_POINT = Gauge('weather_dew_point_celsius', 'Điểm sương', ['city'])

# 3. Các chỉ số thống kê xu hướng (Rolling Stats từ Gold Layer)
# Giúp so sánh nhiệt độ hiện tại với trung bình 24h qua ngay trên biểu đồ
M_TEMP_MEAN_24H = Gauge('weather_temperature_mean_24h_celsius', 'Nhiệt độ trung bình 24h qua', ['city'])
M_TEMP_STD_24H = Gauge('weather_temperature_std_24h_celsius', 'Độ lệch chuẩn nhiệt độ 24h (Độ biến động)', ['city'])

def get_mongo_collection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        return db[COLLECTION_NAME]
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def fetch_latest_gold_data():
    col = get_mongo_collection()
    if col is None:
        return

    try:
        # PIPELINE QUAN TRỌNG:
        # Vì collection 'batch_gold' chứa lịch sử time-series,
        # ta phải Group by City để lấy bản ghi mới nhất (timestamp lớn nhất) cho từng thành phố.
        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$city",
                "latest_doc": {"$first": "$$ROOT"}
            }}
        ]
        
        results = list(col.aggregate(pipeline))
        
        if not results:
            logger.warning("No data found in batch_gold collection.")
            return

        for record in results:
            data = record['latest_doc']
            city = data.get('city', 'Unknown')
            
            # --- Cập nhật Metric (Sử dụng .get() để tránh lỗi nếu field bị null) ---
            
            # Basic
            M_TEMP.labels(city=city).set(data.get('temperature_2m', 0))
            M_HUMIDITY.labels(city=city).set(data.get('relative_humidity_2m', 0))
            M_WIND_SPEED.labels(city=city).set(data.get('wind_speed_10m', 0))
            M_PRECIPITATION.labels(city=city).set(data.get('precipitation', 0))
            M_SOIL_TEMP.labels(city=city).set(data.get('soil_temperature_0_to_7cm', 0))
            M_SOIL_MOISTURE.labels(city=city).set(data.get('soil_moisture_0_to_7cm', 0))

            # Advanced (Tính toán từ silver_to_gold.py)
            M_APP_TEMP.labels(city=city).set(data.get('apparent_temperature', 0))
            M_VPD.labels(city=city).set(data.get('vapor_pressure_deficit', 0))
            M_DEW_POINT.labels(city=city).set(data.get('dew_point_2m', 0))

            # Rolling Stats (Ví dụ lấy window 1d = 24h)
            # Tên field trong DB khớp với logic trong code Spark: {col}_{stat}_{window}
            M_TEMP_MEAN_24H.labels(city=city).set(data.get('temperature_2m_mean_1d', 0))
            M_TEMP_STD_24H.labels(city=city).set(data.get('temperature_2m_std_1d', 0))

        logger.info(f"Updated metrics for {len(results)} cities.")

    except Exception as e:
        logger.error(f"Error fetching data: {e}")

if __name__ == '__main__':
    # Start server
    start_http_server(8000)
    logger.info(f"Weather Gold Exporter running on port 8000. Scrape interval: {SCRAPE_INTERVAL}s")
    logger.info(f"Target DB: {DB_NAME}.{COLLECTION_NAME}")
    
    while True:
        fetch_latest_gold_data()
        time.sleep(SCRAPE_INTERVAL)