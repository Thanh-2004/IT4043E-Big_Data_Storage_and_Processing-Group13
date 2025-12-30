# MongoDB Demo - Batch Processing Weather Data

Demo batch processing dữ liệu thời tiết từ CSV vào MongoDB sử dụng PySpark.

## Files

- `batch_mongo.py` - Script chính xử lý batch
- `config.yaml` - File cấu hình MongoDB và Spark
- `weather_history_2000-01-01_2025-12-05.csv` - Dữ liệu thời tiết lịch sử
- `docker-compose.demo.yaml` - Docker compose để chạy MongoDB standalone

## Cách chạy

### Option 1: Chạy local (MongoDB đã có sẵn)

```powershell
# Cài đặt dependencies
pip install pyspark pymongo pyyaml

# Đảm bảo MongoDB đang chạy trên localhost:27017
# Nếu MongoDB chưa chạy, xem Option 2

# Chạy script
cd demo
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 batch_mongo.py
```

### Option 2: Chạy với Docker Compose

```powershell
# Khởi động MongoDB
cd demo
docker-compose -f docker-compose.demo.yaml up -d

# Đợi MongoDB khởi động (5-10 giây)
timeout 10

# Chạy script
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 batch_mongo.py
```

### Option 3: Chạy hoàn toàn trong Docker

```powershell
cd demo
docker-compose -f docker-compose.demo.yaml up
```

## Kết quả

Script sẽ tạo các collections trong database `weather_demo_db`:

1. **yearly_summary** - Thống kê theo năm
   - avg_temperature, max_temperature, min_temperature
   - avg_humidity, total_precipitation
   - avg_wind_speed, record_count

2. **monthly_summary** - Thống kê theo tháng
   - Tương tự yearly nhưng chi tiết hơn

3. **hottest_days** - Top 10 ngày nóng nhất

4. **raw_weather_data** (optional) - Dữ liệu mẫu gốc

## Kiểm tra kết quả

### Option 1: Dùng Mongo Express Web UI (Đơn giản nhất!)

1. Khởi động Mongo Express:
```powershell
docker-compose -f docker-compose.demo.yaml up -d mongo-express
```

2. Mở trình duyệt: **http://localhost:8081**

3. Đăng nhập:
   - Username: `admin`
   - Password: `admin123`

4. Click vào database `weather_demo_db` để xem các collections

### Option 2: Dùng MongoDB Compass

1. Kết nối: `mongodb://localhost:27017`
2. Chọn database: `weather_demo_db`
3. Xem các collections

### Option 3: Dùng mongosh (Command line)

```bash
mongosh mongodb://localhost:27017

use weather_demo_db
db.yearly_summary.find().pretty()
db.monthly_summary.find().limit(5).pretty()
db.hottest_days.find().pretty()
```

## Cấu hình

Sửa file `config.yaml`:

```yaml
mongodb:
  uri: "mongodb://localhost:27017"  # Đổi nếu MongoDB khác host
  database: "weather_demo_db"       # Đổi tên database
  write_mode: "overwrite"           # overwrite hoặc append
  save_raw_data: false              # true để lưu raw data
  sample_size: 1000                 # Số records mẫu nếu save_raw_data=true
```

## Troubleshooting

### Lỗi: Cannot connect to MongoDB

```
Kiểm tra MongoDB đang chạy:
docker ps | grep mongodb
```

### Lỗi: Spark connector not found

```powershell
# Đảm bảo có --packages khi chạy
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 batch_mongo.py
```

### Lỗi: Module yaml not found

```powershell
pip install pyyaml
```
