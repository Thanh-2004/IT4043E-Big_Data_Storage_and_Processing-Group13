import os, time, json, requests, datetime
from kafka import KafkaProducer
from datetime import timedelta

BROKER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")

HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
GLOBAL_START_DATE = "2020-01-01"
GLOBAL_END_DATE = "2025-12-31"

# List of cities
CITIES = [
    # ========== VÙNG 1: MIỀN BẮC ==========
    {"name": "Lai Châu (tây bắc)", "lat": 23.00, "lon": 102.80},
    {"name": "Lai Châu - Lào Cai", "lat": 23.00, "lon": 103.50},
    {"name": "Lào Cai", "lat": 23.00, "lon": 104.20},
    {"name": "Hà Giang (tây)", "lat": 22.90, "lon": 104.90},
    {"name": "Hà Giang (trung tâm)", "lat": 22.90, "lon": 105.60},
    {"name": "Cao Bằng", "lat": 22.80, "lon": 106.30},
    {"name": "Lạng Sơn (bắc)", "lat": 22.70, "lon": 107.00},
    {"name": "Điện Biên (bắc)", "lat": 22.30, "lon": 103.00},
    {"name": "Điện Biên - Lào Cai", "lat": 22.30, "lon": 103.70},
    {"name": "Yên Bái (bắc)", "lat": 22.30, "lon": 104.40},
    {"name": "Tuyên Quang (tây)", "lat": 22.20, "lon": 105.10},
    {"name": "Tuyên Quang", "lat": 22.10, "lon": 105.80},
    {"name": "Bắc Kạn", "lat": 22.00, "lon": 106.50},
    {"name": "Lạng Sơn (nam)", "lat": 21.90, "lon": 107.20},
    {"name": "Sơn La (bắc)", "lat": 21.60, "lon": 103.20},
    {"name": "Sơn La (đông)", "lat": 21.60, "lon": 103.90},
    {"name": "Yên Bái", "lat": 21.60, "lon": 104.60},
    {"name": "Phú Thọ (tây)", "lat": 21.50, "lon": 105.30},
    {"name": "Thái Nguyên", "lat": 21.50, "lon": 106.00},
    {"name": "Bắc Giang", "lat": 21.40, "lon": 106.70},
    {"name": "Quảng Ninh (tây)", "lat": 21.30, "lon": 107.40},
    {"name": "Sơn La (nam)", "lat": 20.90, "lon": 103.50},
    {"name": "Hòa Bình (tây)", "lat": 20.90, "lon": 104.20},
    {"name": "Hòa Bình", "lat": 20.90, "lon": 104.90},
    {"name": "Vĩnh Phúc Hà Nội (tây)", "lat": 20.80, "lon": 105.60},
    {"name": "Bắc Ninh Hải Dương", "lat": 20.80, "lon": 106.30},
    {"name": "Hải Phòng - Quảng Ninh", "lat": 20.70, "lon": 107.00},
    {"name": "Ninh Bình (tây)", "lat": 20.20, "lon": 105.00},
    {"name": "Hà Nam - Nam Định (tây)", "lat": 20.10, "lon": 105.70},
    {"name": "Nam Định - Thái Bình", "lat": 20.00, "lon": 106.40},
    {"name": "Khu vực ven biển Bắc Bộ", "lat": 19.90, "lon": 107.10},
    {"name": "Thanh Hóa (bắc tây)", "lat": 19.50, "lon": 105.20},
    {"name": "Thanh Hóa (bắc)", "lat": 19.40, "lon": 105.90},
    {"name": "Thanh Hóa (ven biển)", "lat": 19.30, "lon": 106.60},

    # ========== VÙNG 2: BẮC TRUNG BỘ ==========
    {"name": "Thanh Hóa (tây nam)", "lat": 18.80, "lon": 104.80},
    {"name": "Nghệ An (tây)", "lat": 18.70, "lon": 105.50},
    {"name": "Nghệ An (Vinh)", "lat": 18.60, "lon": 106.20},
    {"name": "Nghệ An (ven biển)", "lat": 18.50, "lon": 106.90},
    {"name": "Nghệ An (tây nam)", "lat": 18.10, "lon": 105.10},
    {"name": "Hà Tĩnh (tây)", "lat": 18.00, "lon": 105.80},
    {"name": "Hà Tĩnh", "lat": 17.90, "lon": 106.50},
    {"name": "Hà Tĩnh (ven biển)", "lat": 17.80, "lon": 107.20},
    {"name": "Quảng Bình (tây)", "lat": 17.40, "lon": 105.50},
    {"name": "Quảng Bình", "lat": 17.30, "lon": 106.20},
    {"name": "Quảng Bình (đông)", "lat": 17.20, "lon": 106.90},
    {"name": "Quảng Bình (ven biển)", "lat": 17.10, "lon": 107.60},
    {"name": "Quảng Trị (tây)", "lat": 16.70, "lon": 106.30},
    {"name": "Quảng Trị", "lat": 16.60, "lon": 107.00},
    {"name": "Quảng Trị (ven biển)", "lat": 16.50, "lon": 107.70},
    {"name": "Thừa Thiên Huế (tây)", "lat": 16.10, "lon": 107.00},
    {"name": "Huế", "lat": 16.00, "lon": 107.70},
    {"name": "Huế (ven biển)", "lat": 15.90, "lon": 108.40},
    {"name": "Quảng Nam (tây)", "lat": 15.50, "lon": 107.40},
    {"name": "Đà Nẵng - Quảng Nam", "lat": 15.40, "lon": 108.10},
    {"name": "Quảng Nam (ven biển)", "lat": 15.30, "lon": 108.80},
    {"name": "Quảng Nam (nam)", "lat": 14.90, "lon": 108.00},
    {"name": "Quảng Ngãi (bắc)", "lat": 14.80, "lon": 108.70},
    {"name": "Quảng Ngãi (ven biển)", "lat": 14.70, "lon": 109.40},

    # ========== VÙNG 3: TÂY NGUYÊN ==========
    {"name": "Kon Tum (tây bắc)", "lat": 14.80, "lon": 107.30},
    {"name": "Gia Lai (tây bắc)", "lat": 14.10, "lon": 107.30},
    {"name": "Gia Lai (tây)", "lat": 13.40, "lon": 107.40},
    {"name": "Đắk Lắk (tây)", "lat": 12.70, "lon": 107.50},
    {"name": "Lâm Đồng (tây bắc)", "lat": 12.00, "lon": 107.60},
    {"name": "Lâm Đồng (tây nam)", "lat": 11.30, "lon": 107.70},
    {"name": "Kon Tum", "lat": 14.50, "lon": 108.00},
    {"name": "Gia Lai (Pleiku)", "lat": 13.80, "lon": 108.10},
    {"name": "Đắk Lắk (Buôn Ma Thuột)", "lat": 13.10, "lon": 108.20},
    {"name": "Đắk Nông", "lat": 12.40, "lon": 108.30},
    {"name": "Lâm Đồng (Đà Lạt)", "lat": 11.70, "lon": 108.40},
    {"name": "Lâm Đồng (nam)", "lat": 11.00, "lon": 108.50},
    {"name": "Gia Lai (đông)", "lat": 14.20, "lon": 108.70},
    {"name": "Đắk Lắk (đông)", "lat": 13.50, "lon": 108.80},
    {"name": "Đắk Nông (đông)", "lat": 12.80, "lon": 108.90},
    {"name": "Lâm Đồng (đông)", "lat": 12.10, "lon": 109.00},
    {"name": "Ninh Thuận - Bình Thuận (tây)", "lat": 11.40, "lon": 109.10},
    {"name": "Bình Thuận (tây nam)", "lat": 10.70, "lon": 108.20},

    # ========== VÙNG 4: NAM TRUNG BỘ ==========
    {"name": "Bình Định (tây)", "lat": 14.10, "lon": 109.10},
    {"name": "Bình Định (Quy Nhơn)", "lat": 14.00, "lon": 109.80},
    {"name": "Phú Yên (tây)", "lat": 13.40, "lon": 109.20},
    {"name": "Phú Yên (Tuy Hòa)", "lat": 13.30, "lon": 109.90},
    {"name": "Khánh Hòa (tây)", "lat": 12.70, "lon": 109.30},
    {"name": "Khánh Hòa (Nha Trang)", "lat": 12.50, "lon": 109.90},
    {"name": "Ninh Thuận (trung tâm)", "lat": 12.00, "lon": 109.20},
    {"name": "Ninh Thuận (Phan Rang)", "lat": 11.80, "lon": 109.80},
    {"name": "Bình Thuận (tây)", "lat": 11.20, "lon": 108.70},
    {"name": "Bình Thuận (Phan Thiết)", "lat": 11.00, "lon": 109.30},
    {"name": "Bình Thuận (ven biển đông)", "lat": 10.80, "lon": 109.90},
    {"name": "Bình Thuận (cực nam)", "lat": 10.20, "lon": 108.80},

    # ========== VÙNG 5: ĐÔNG NAM BỘ ==========
    {"name": "Bình Phước (bắc)", "lat": 11.70, "lon": 106.50},
    {"name": "Bình Phước (đông bắc)", "lat": 11.50, "lon": 107.20},
    {"name": "Tây Ninh (tây)", "lat": 11.20, "lon": 106.00},
    {"name": "Tây Ninh - Bình Dương", "lat": 11.00, "lon": 106.70},
    {"name": "Đồng Nai (bắc)", "lat": 11.00, "lon": 107.40},
    {"name": "Bình Dương (nam)", "lat": 10.70, "lon": 106.30},
    {"name": "TP.HCM - Đồng Nai", "lat": 10.50, "lon": 107.00},
    {"name": "Đồng Nai (đông)", "lat": 10.30, "lon": 107.70},
    {"name": "Đồng Nai (nam đông)", "lat": 10.60, "lon": 107.40},
    {"name": "Bà Rịa - Vũng Tàu (tây)", "lat": 10.30, "lon": 107.10},
    {"name": "Vũng Tàu", "lat": 10.10, "lon": 107.60},
    {"name": "TP.HCM (bắc)", "lat": 10.80, "lon": 106.70},
    {"name": "TP.HCM (tây)", "lat": 10.50, "lon": 106.50},
    {"name": "TP.HCM (trung tâm)", "lat": 10.30, "lon": 106.70},
    {"name": "TP.HCM (nam)", "lat": 10.10, "lon": 106.40},

    # ========== VÙNG 6: ĐỒNG BẰNG SÔNG CỬU LONG ==========
    {"name": "Long An (bắc tây)", "lat": 10.60, "lon": 105.50},
    {"name": "Long An", "lat": 10.50, "lon": 106.10},
    {"name": "Tiền Giang (tây)", "lat": 10.40, "lon": 106.70},
    {"name": "An Giang (bắc)", "lat": 10.20, "lon": 105.20},
    {"name": "Đồng Tháp (bắc)", "lat": 10.10, "lon": 105.80},
    {"name": "Tiền Giang - Vĩnh Long", "lat": 10.00, "lon": 106.40},
    {"name": "Bến Tre (bắc)", "lat": 9.90, "lon": 107.00},
    {"name": "An Giang (Long Xuyên)", "lat": 9.80, "lon": 104.90},
    {"name": "Đồng Tháp (Cao Lãnh)", "lat": 9.70, "lon": 105.50},
    {"name": "Vĩnh Long", "lat": 9.60, "lon": 106.10},
    {"name": "Bến Tre (trung tâm)", "lat": 9.50, "lon": 106.70},
    {"name": "Kiên Giang (đông bắc)", "lat": 9.40, "lon": 105.10},
    {"name": "Hậu Giang (bắc)", "lat": 9.30, "lon": 105.70},
    {"name": "Trà Vinh (bắc)", "lat": 9.20, "lon": 106.30},
    {"name": "Kiên Giang (Rạch Giá - bắc)", "lat": 9.10, "lon": 104.80},
    {"name": "Cần Thơ", "lat": 9.00, "lon": 105.40},
    {"name": "Hậu Giang", "lat": 8.90, "lon": 106.00},
    {"name": "Trà Vinh (nam)", "lat": 8.80, "lon": 106.60},
    {"name": "Kiên Giang (Rạch Giá)", "lat": 8.70, "lon": 105.10},
    {"name": "Sóc Trăng (tây)", "lat": 8.60, "lon": 105.70},
    {"name": "Sóc Trăng", "lat": 8.50, "lon": 106.30},
    {"name": "Kiên Giang (nam)", "lat": 8.30, "lon": 104.80},
    {"name": "Bạc Liêu (bắc)", "lat": 8.20, "lon": 105.40},
    {"name": "Bạc Liêu", "lat": 8.10, "lon": 106.00},
    {"name": "Sóc Trăng (cực nam)", "lat": 8.00, "lon": 106.60},
    {"name": "Kiên Giang (ven biển tây)", "lat": 9.00, "lon": 104.50},
    {"name": "Kiên Giang (Phú Quốc)", "lat": 8.70, "lon": 104.50},
    {"name": "Cà Mau (tây bắc)", "lat": 8.50, "lon": 104.90},
    {"name": "Cà Mau (bắc)", "lat": 8.80, "lon": 105.00},
    {"name": "Cà Mau (trung tâm)", "lat": 8.60, "lon": 105.20},
    {"name": "Cà Mau (đông)", "lat": 8.50, "lon": 105.50}
]

def fetch_weather(lat, lon, start_date, end_date):
    url = (
        f"{HISTORICAL_URL}?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}"
        "&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,precipitation,"
        "cloud_cover,sunshine_duration,wind_speed_10m,wind_direction_10m,weather_code,"
        "soil_temperature_0_to_7cm,soil_moisture_0_to_7cm&timezone=UTC"
    )
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Faild to fetch data: {e}")
        return None
        

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    global_start = datetime.datetime.strptime(GLOBAL_START_DATE, "%Y-%m-%d")
    global_end = datetime.datetime.strptime(GLOBAL_END_DATE, "%Y-%m-%d")

    print(f"🚀 Starting producer. Ingesting from {GLOBAL_START_DATE} to {GLOBAL_END_DATE}")

    for city in CITIES:
        print(f"\n Processing City: {city['name'].upper()}")

        current_chunk_start = global_start

        while current_chunk_start <= global_end:
            # Request for one-year data at a time
            current_chunk_end = current_chunk_start + timedelta(days=364)
        
            if current_chunk_end > global_end:
                current_chunk_end = global_end

            s_str = current_chunk_start.strftime("%Y-%m-%d")
            e_str = current_chunk_end.strftime("%Y-%m-%d")

            print(f"Fetching chunk: {s_str} -> {e_str} ... ", end="")
            
            data = fetch_weather(city["lat"], city["lon"], s_str, e_str)

            if (not data) or ("hour" not in data):
                print(f"No data for city {city["name"]}!")
                continue
            
            record_cnt = 0
            for i, t in enumerate(data["hourly"]["time"]):
                record = {
                    "city": city["name"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "timestamp": t,
                    "temperature_2m": data["hourly"]["temperature_2m"][i],
                    "relative_humidity_2m": data["hourly"]["relative_humidity_2m"][i],
                    "dew_point_2m": data["hourly"]["dew_point_2m"][i],
                    "precipitation": data["hourly"]["precipitation"][i],
                    "cloud_cover": data["hourly"]["cloud_cover"][i],
                    "sunshine_duration": data["hourly"]["sunshine_duration"][i],
                    "wind_speed_10m": data["hourly"]["wind_speed_10m"][i],
                    "wind_direction_10m": data["hourly"]["wind_direction_10m"][i],
                    "weather_code": data["hourly"]["weather_code"][i],
                    "soil_temperature_0_to_7cm": data["hourly"]["soil_temperature_0_to_7cm"][i],
                    "soil_moisture_0_to_7cm": data["hourly"]["soil_moisture_0_to_7cm"][i],
                    "ingested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "source": "open-meteo"
                }
                producer.send(TOPIC, record)
                record_cnt += 1
            producer.flush()
            print(f"Batch of size {record_cnt} sent to Kafka. Waiting 30 seconds...")
            
            if current_chunk_start == global_end:
                break

            current_chunk_start = current_chunk_end + timedelta(days=1)
            if current_chunk_start > global_end:
                current_chunk_start = global_end
            
            # Be polite to the API
            time.sleep(3)
        print(f"CITY {city["name"].upper()} COMPLETED!")
    print("All CITIES COMPLETED!")
    time.sleep(30)