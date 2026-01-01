"""
WORKFLOW:
Given a list of cities and desired weather features, crawl data from Open-Meteo API

USAGE:
python download_openmeteo_historical_data.py --start_date 20000101 --end_date 20251205
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

OUTPUT_DIR = './big_data'

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
    {"name": "Hanoi", "lat": 20.32773, "lon": 106.0128}
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