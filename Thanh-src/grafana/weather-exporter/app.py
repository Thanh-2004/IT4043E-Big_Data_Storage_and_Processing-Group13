import time
import os
from pymongo import MongoClient
from prometheus_client import start_http_server, Gauge

# 1. Cấu hình kết nối Mongo
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/')
DB_NAME = os.getenv('DB_NAME', 'weather_db')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'daily_weather')

# 2. Định nghĩa Metrics cho Prometheus
# Gauge là loại biểu đồ đo lường giá trị lên xuống (như nhiệt kế, tốc độ xe)
TEMP_GAUGE = Gauge('weather_temperature_celsius', 'Nhiệt độ hiện tại', ['city'])
HUMIDITY_GAUGE = Gauge('weather_humidity_percent', 'Độ ẩm hiện tại', ['city'])

def fetch_data():
    try:
        # Kết nối DB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]

        # Lấy bản ghi mới nhất (sắp xếp theo thời gian giảm dần)
        # Giả sử trong Mongo bạn có trường 'timestamp' hoặc '_id' để sort
        latest_data = col.find_one(sort=[('_id', -1)]) 

        if latest_data:
            city = latest_data.get('city', 'Unknown')
            temp = latest_data.get('temperature', 0)
            hum = latest_data.get('humidity', 0)

            # Cập nhật giá trị cho Prometheus
            TEMP_GAUGE.labels(city=city).set(temp)
            HUMIDITY_GAUGE.labels(city=city).set(hum)
            
            print(f"Updated: City={city}, Temp={temp}, Hum={hum}")
        else:
            print("No data found in MongoDB")

        client.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    # Khởi chạy server metrics ở cổng 8000
    start_http_server(8000)
    print("Weather Exporter running on port 8000...")
    
    while True:
        fetch_data()
        time.sleep(15) # Cập nhật mỗi 15 giây