"""
WORKFLOW:
Given a list of cities and desired weather features, crawl data from Open-Meteo API

USAGE:
python download_openmeteo_historical_data.py --start_date 20200101 --end_date 20251231
"""

import os
import gc
import time
import logging
import argparse
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timedelta

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', # printf-style formatting: %(name)[type]
    level=logging.INFO # Minimum level to log
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = './raw-data'

HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = [
    'temperature_2m',
    'relative_humidity_2m',
    'dew_point_2m',
    'precipitation',
    'cloud_cover',
    'sunshine_duration',
    'wind_speed_10m',
    'wind_direction_10m',
    'weather_code',
    'soil_temperature_0_to_7cm',
    'soil_moisture_0_to_7cm'
]

CITIES = [
    # ========== VÙNG 1: MIỀN BẮC ==========
    # {"name": "Lai Châu (tây bắc)", "lat": 23.00, "lon": 102.80},
    # {"name": "Lai Châu - Lào Cai", "lat": 23.00, "lon": 103.50},
    # {"name": "Lào Cai", "lat": 23.00, "lon": 104.20},
    # {"name": "Hà Giang (tây)", "lat": 22.90, "lon": 104.90},
    # {"name": "Hà Giang (trung tâm)", "lat": 22.90, "lon": 105.60},
    # {"name": "Cao Bằng", "lat": 22.80, "lon": 106.30},
    # {"name": "Lạng Sơn (bắc)", "lat": 22.70, "lon": 107.00},
    # {"name": "Điện Biên (bắc)", "lat": 22.30, "lon": 103.00},
    # {"name": "Điện Biên - Lào Cai", "lat": 22.30, "lon": 103.70},
    # {"name": "Yên Bái (bắc)", "lat": 22.30, "lon": 104.40},
    # {"name": "Tuyên Quang (tây)", "lat": 22.20, "lon": 105.10},
    # {"name": "Tuyên Quang", "lat": 22.10, "lon": 105.80},
    # {"name": "Bắc Kạn", "lat": 22.00, "lon": 106.50},
    # {"name": "Lạng Sơn (nam)", "lat": 21.90, "lon": 107.20},
    # {"name": "Sơn La (bắc)", "lat": 21.60, "lon": 103.20},
    # {"name": "Sơn La (đông)", "lat": 21.60, "lon": 103.90},
    # {"name": "Yên Bái", "lat": 21.60, "lon": 104.60},
    # {"name": "Phú Thọ (tây)", "lat": 21.50, "lon": 105.30},
    # {"name": "Thái Nguyên", "lat": 21.50, "lon": 106.00},
    # {"name": "Bắc Giang", "lat": 21.40, "lon": 106.70},
    # {"name": "Quảng Ninh (tây)", "lat": 21.30, "lon": 107.40},
    # {"name": "Sơn La (nam)", "lat": 20.90, "lon": 103.50},
    # {"name": "Hòa Bình (tây)", "lat": 20.90, "lon": 104.20},
    # {"name": "Hòa Bình", "lat": 20.90, "lon": 104.90},
    # {"name": "Vĩnh Phúc Hà Nội (tây)", "lat": 20.80, "lon": 105.60},
    # {"name": "Bắc Ninh Hải Dương", "lat": 20.80, "lon": 106.30},
    # {"name": "Hải Phòng - Quảng Ninh", "lat": 20.70, "lon": 107.00},
    # {"name": "Ninh Bình (tây)", "lat": 20.20, "lon": 105.00},
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

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download historical weather data from Open-Meteo"
    )

    parser.add_argument(
        '--start_date',
        type=str,
        required=True,
        help='First date to start crawling weather data (format: YYYYMMDD)'
    )

    parser.add_argument(
        '--end_date',
        type=str,
        required=True,
        help='Last date to crawl weather data from (format: YYYYMMDD)'
    )

    args = parser.parse_args()
    return args

def main():
    # Make sure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    args = parse_arguments()
    if (int(args.start_date) > int(args.end_date)):
        logger.error(f"start_date must be before end_date: {args.start_date} > {args.end_date}")

    global_start_date = datetime.strptime(args.start_date, "%Y%m%d")
    global_end_date = datetime.strptime(args.end_date, "%Y%m%d")
    
    logger.info('=' * 70)
    logger.info('STEP 1: Setup the Open-Meteo API client with cache and retry on error')
    logger.info('=' * 70)

    try:
        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

    except requests_cache.exceptions.CacheError as e:
        logging.error("Cache initialization failed: %s", e)
        raise

    except Exception as e:
        logging.exception("Failed to initialize Open-Meteo client")
        raise

    logger.info("SUCCESSFULLY SET UP Open-Meteo API CLIENT!\n")
    
    logger.info("=" * 70)
    logger.info(f"STEP 2: Requesting data for {len(CITIES)} cities...")
    logger.info("=" * 70)

    for city in CITIES:
        city_name = city.get("name", "")
        latitude = city.get("lat", None)
        longitude = city.get("lon", None)

        filename = f"{OUTPUT_DIR}/{city_name}_{args.start_date}_{args.end_date}.csv"

        if os.path.exists(filename):
            logger.warning(f"{filename} already existed, will be overwritten!")
            os.remove(filename)
        
        time_chunk_start = global_start_date
        while time_chunk_start <= global_end_date:
            time_chunk_end = time_chunk_start + timedelta(days=364)
            if time_chunk_end > global_end_date:
                time_chunk_end = global_end_date
            
            start_str = time_chunk_start.strftime("%Y-%m-%d")
            end_str = time_chunk_end.strftime("%Y-%m-%d")

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_str,
                "end_date": end_str,
                "hourly": HOURLY_VARS,
                "timezone": "Asia/Bangkok"
            }

            try:
                responses = openmeteo.weather_api(HISTORICAL_URL, params=params)
            except Exception as e:
                logger.exception(f"Failed to request data from {HISTORICAL_URL} from {start_str} to {end_str} for {city_name.upper()}")
                raise
            logger.info(f"SUCCESSFULLY REQUESTED DATA FROM {start_str} TO {end_str} FOR {city_name.upper()}!\n")

            logger.info("=" * 70)
            logger.info(f"Processing and downloading for {city_name.upper()}: {start_str} - {end_str}...")
            logger.info("=" * 70)

            response = responses[0]

            hourly = response.Hourly()
            hourly_data = {
                "timestamp": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    freq = pd.Timedelta(seconds = hourly.Interval()),
                    inclusive = "left"
                )
            }

            for i, feature in enumerate(HOURLY_VARS):
                hourly_data[feature] = hourly.Variables(i).ValuesAsNumpy()
            
            hourly_dataframe = pd.DataFrame(data=hourly_data)
            hourly_dataframe["city"] = city_name
            hourly_dataframe.info()
            logger.info(f"SUCCESSFULLY PROCESSED DATA FOR {city_name.upper()}: {start_str} - {end_str}\n")

            # Save to CSV + append time chunk data
            write_header = not os.path.exists(filename)

            hourly_dataframe.to_csv(
                filename,
                header=write_header,
                index=False, 
                mode ='a',
                chunksize=100_000
            )
            logger.info(f"SAVED TO {filename}: {start_str} - {end_str}!\n")

            # Free RAM
            del hourly_dataframe
            gc.collect()

            # Break the time-chunk data
            if time_chunk_start == global_end_date:
                break
            
            # Move to the next time chunk
            time_chunk_start = time_chunk_end + timedelta(days=1)
            if time_chunk_start > global_end_date:
                time_chunk_start = global_end_date
            
            # Be polite to the API
            time.sleep(1)

        logger.info(f"COMPLETED DOWNLOADING {city_name.upper()}!")
        
    logger.info("DOWNLOADING PROCESS COMPLETED!")
if __name__ == '__main__':
    main()