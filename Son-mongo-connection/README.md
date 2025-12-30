# MongoDB Demo - PySpark Batch & Stream Processing

Demo xử lý dữ liệu thời tiết vào MongoDB với PySpark - hỗ trợ cả Batch và Stream processing.

## Files

- `batch_mongo.py` - Batch processing (one-time execution)
- `stream_mongo.py` - Stream processing (continuous execution)
- `config.yaml` - Cấu hình MongoDB và Spark
- `weather_history_2000-01-01_2025-12-05.csv` - Dữ liệu thời tiết
- `docker-compose.demo.yaml` - Docker MongoDB

## Quick Start

### 1. Setup Environment (Windows)

```powershell
$env:PYSPARK_PYTHON = "C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
pip install pyspark pymongo pyyaml
```

### 2. Start MongoDB

```powershell
docker-compose -f docker-compose.demo.yaml up -d
```

### 3. Run Processing

**Batch Mode** (chạy 1 lần rồi dừng):
```powershell
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 batch_mongo.py
```

**Stream Mode** (chạy liên tục):
```powershell
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 stream_mongo.py

# Terminal khác: Copy file vào để trigger
cp weather_history_2000-01-01_2025-12-05.csv input_stream/test.csv
```

## Stream Processing - Chi tiết

### Bước 1: Khởi động stream
```powershell
# Chạy stream (sẽ chờ file mới liên tục)
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 stream_mongo.py
```

Output sẽ hiển thị:
```
============================================================
DEMO: Stream Processing Weather Data to MongoDB
============================================================
[STREAM] Input Directory: C:\...\input_stream
[CHECKPOINT] Directory: C:\...\checkpoints
[MONGO] URI: mongodb://localhost:27017
[DB] Database: weather_demo_db
============================================================
[INFO] Waiting for new CSV files in input_stream folder...
[INFO] Copy CSV files to input_stream folder to process
[INFO] Press Ctrl+C to stop streaming
============================================================
```

### Bước 2: Trigger processing (Terminal mới)

**Option 1 - Copy file gốc:**
```powershell
cp weather_history_2000-01-01_2025-12-05.csv input_stream/data1.csv
```

**Option 2 - Copy subset nhỏ để test:**
```powershell
# Lấy 1000 dòng đầu
Get-Content weather_history_2000-01-01_2025-12-05.csv -TotalCount 1000 | Set-Content input_stream/small.csv
```

**Option 3 - Simulate real-time data:**
```powershell
# Copy nhiều file nhỏ liên tục
for ($i=1; $i -le 5; $i++) {
    Get-Content weather_history_2000-01-01_2025-12-05.csv -TotalCount 500 | Set-Content "input_stream/batch_$i.csv"
    Start-Sleep -Seconds 15
}
```

### Bước 3: Monitor output

Terminal stream sẽ hiển thị:
```
[BATCH 0] Written 999 records to stream_raw_data
[BATCH 0] Updated yearly aggregations
[BATCH 0] Updated monthly aggregations
[BATCH 0] Updated hourly windowed aggregations
```

### Bước 4: Stop stream

Nhấn `Ctrl+C` trong terminal đang chạy stream:
```
^C
[STOP] Stopping all streams...
[OK] All streams stopped successfully!
============================================================
```

### Useful Commands

**Check processed files:**
```powershell
ls input_stream/
```

**Clear input folder:**
```powershell
Remove-Item input_stream/*.csv
```

**Reset checkpoint (chạy lại từ đầu):**
```powershell
# Dừng stream trước (Ctrl+C)
Remove-Item -Recurse -Force checkpoints/*
# Chạy lại stream
```

**Monitor MongoDB real-time:**
```powershell
# Terminal riêng
mongosh mongodb://localhost:27017
use weather_demo_db
# Chạy liên tục để xem count tăng
while(true) { 
    print(new Date(), "- Raw:", db.stream_raw_data.countDocuments()); 
    sleep(3000); 
}
```

## Batch vs Stream

| | Batch | Stream |
|---|---|---|
| **Execution** | One-time | Continuous |
| **Input** | Full CSV file | Files in `input_stream/` |
| **Output Collections** | yearly_summary<br>monthly_summary<br>hottest_days | stream_raw_data<br>stream_yearly_summary<br>stream_monthly_summary<br>stream_hourly_summary |
| **Write Mode** | Overwrite/Append | Append only |
| **Checkpointing** | ❌ | ✅ (in `checkpoints/`) |
| **Use Case** | Historical analysis | Real-time monitoring |

## Collections Created

### Batch Collections
- `yearly_summary` - Thống kê theo năm
- `monthly_summary` - Thống kê theo tháng  
- `hottest_days` - Top 10 ngày nóng nhất

### Stream Collections
- `stream_raw_data` - Raw data (trigger: 10s)
- `stream_yearly_summary` - Yearly agg (trigger: 30s)
- `stream_monthly_summary` - Monthly agg (trigger: 30s)
- `stream_hourly_summary` - Hourly windows (trigger: 30s, watermark: 2h)

## Check Results

**Mongo Express** (Web UI): http://localhost:8081
- Username: `admin` / Password: `admin123`

**MongoDB Compass**: `mongodb://localhost:27017`

**mongosh**:
```bash
mongosh mongodb://localhost:27017
use weather_demo_db
db.yearly_summary.find().pretty()
db.stream_raw_data.countDocuments()
```

## Configuration

Edit `config.yaml`:

```yaml
mongodb:
  uri: "mongodb://localhost:27017"
  database: "weather_demo_db"
  write_mode: "overwrite"  # batch only
  save_raw_data: false
  sample_size: 1000
```

## Troubleshooting

**MongoDB không connect được:**
```powershell
docker ps | grep mongodb
```

**Reset stream checkpoints:**
```powershell
Remove-Item -Recurse -Force checkpoints/*
```

**Xóa collections:**
```bash
mongosh mongodb://localhost:27017
use weather_demo_db
db.dropDatabase()
```
