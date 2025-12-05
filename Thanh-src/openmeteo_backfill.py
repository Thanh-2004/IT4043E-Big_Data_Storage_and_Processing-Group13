import requests
import pandas as pd
from datetime import date
import time

# 1. Cấu hình
# Tọa độ Hà Nội (Bạn có thể đổi sang nơi khác)
LATITUDE = 21.0285
LONGITUDE = 105.8542
START_DATE = "2000-01-01"
END_DATE = date.today().strftime("%Y-%m-%d") # Lấy ngày hôm nay tự động

# URL dành cho dữ liệu lịch sử (Archive API)
url = "https://archive-api.open-meteo.com/v1/archive"

# Các thông số quan trọng cho Data Science
params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "hourly": ["temperature_2m", "precipitation", "relative_humidity_2m", "wind_speed_10m", "weather_code"],
    "timezone": "auto" # Tự động theo múi giờ địa phương (Asia/Bangkok)
}

print(f"🔄 Đang tải dữ liệu từ {START_DATE} đến {END_DATE}...")
print("⏳ Việc này có thể mất vài giây vì lượng dữ liệu lớn (>200.000 dòng)...")

# 2. Gửi Request
try:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status() # Báo lỗi nếu kết nối hỏng
    data = response.json()
    
    # 3. Xử lý dữ liệu vào Pandas DataFrame
    hourly = data.get('hourly', {})
    
    df = pd.DataFrame({
        "Date": hourly['time'],
        "Temperature (°C)": hourly['temperature_2m'],
        "Precipitation (mm)": hourly['precipitation'],
        "Humidity (%)": hourly['relative_humidity_2m'],
        "Wind Speed (km/h)": hourly['wind_speed_10m'],
        "Weather Code": hourly['weather_code']
    })
    
    # Chuyển cột Date sang dạng datetime để dễ filter sau này
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 4. Lưu file
    filename = f"weather_history_{START_DATE}_{END_DATE}.csv"
    df.to_csv(filename, index=False)
    
    print("\n✅ THÀNH CÔNG!")
    print(f"📊 Tổng số bản ghi: {len(df):,}") # In số có dấu phẩy ngăn cách
    print(f"💾 Đã lưu vào file: {filename}")
    print("\n--- 5 Dòng đầu tiên ---")
    print(df.head())

except requests.exceptions.RequestException as e:
    print(f"❌ Lỗi kết nối API: {e}")
except KeyError as e:
    print(f"❌ Lỗi dữ liệu: Không tìm thấy trường {e} trong phản hồi.")
except Exception as e:
    print(f"❌ Lỗi không xác định: {e}")