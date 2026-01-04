import os
import sys

# Get to the project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)

import math
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils.spark_action import create_k8s_spark
from src.utils.data_constants import WINDOW_SIZES, ROLLING_FEATURES, LAGGING_FEATURES, TWO_PI, HOURS_PER_DAY, DAYS_PER_YEAR, KHM_TO_MS

import logging
# Setting up logging configurations
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def log_step_headline(message: str, border_char='=') -> None:
    logger.info(border_char * 70)
    logger.info(message)
    logger.info(border_char * 70)
    return None


def silver_gold_transform(silver_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    feature_exprs = [F.col(col_name) for col_name in silver_df.columns]

    log_step_headline(message="STEP 3.1: Extract cyclical time encoding", border_char='-')
    hour_col = F.hour("timestamp")
    day_of_year_col = F.dayofyear("timestamp")

    feature_exprs.extend(
        [
            F.sin(TWO_PI * hour_col / HOURS_PER_DAY).alias("hour_sin"),
            F.cos(TWO_PI * hour_col / HOURS_PER_DAY).alias("hour_cos"),
            F.sin(TWO_PI * day_of_year_col / DAYS_PER_YEAR).alias("day_of_year_sin"),
            F.cos(TWO_PI * day_of_year_col / DAYS_PER_YEAR).alias("day_of_year_cos")
        ]
    )
    logger.info("CREATE Hour AND Day of Year's Sine AND Cosine COMPONENTS!\n")

    log_step_headline(message="STEP 3.2: Process lagged features", border_char='-')

    available_lag_features = [feature for feature in LAGGING_FEATURES if feature in silver_df.columns]
    logger.info(f"Found {len(available_lag_features)} lagging features: {available_lag_features}!")
    lag_hours = [1, 2, 3, 24]
    w_lag = Window.partitionBy("city").orderBy("timestamp")

    for i, col_name in enumerate(available_lag_features, 1):
        c = F.col(col_name)
        logger.info(f"Finding lagged records for {col_name} ({i}/{len(available_lag_features)})")
        for hour in lag_hours:
            lag_col_name = f"{col_name}_lag_{hour}hrs"
            feature_exprs.append(
                F.lag(c, hour).over(w_lag).alias(lag_col_name)
            )
            logger.info(f"Finish processing {hour}-lag feature: {lag_col_name}")
        logger.info(f"Done finding lagged reocords for {col_name}")
    logger.info('\n')

    log_step_headline(message="STEP 3.3: Discover data trends", border_char='-')
    
    available_rolling_features = [feature for feature in ROLLING_FEATURES if feature in silver_df.columns]
    logger.info(f"Found {len(available_rolling_features)} rolling features: {available_rolling_features}")

    rolling_windows = {
        "12hrs": Window.partitionBy("city").orderBy("timestamp").rowsBetween(-12, -1),
        "1d": Window.partitionBy("city").orderBy("timestamp").rowsBetween(-24, -1),
        "3ds": Window.partitionBy("city").orderBy("timestamp").rowsBetween(-72, -1),
        "1w":  Window.partitionBy("city").orderBy("timestamp").rowsBetween(-168, -1)
    }

    for window_name, w_rolling in rolling_windows.items():
        logger.info(f"Starting rolling back {window_name}...")
        for col_name in available_rolling_features:
            c= F.col(col_name)
            feature_exprs.extend([
                F.avg(c).over(w_rolling).alias(f"{col_name}_mean_{window_name}"),
                F.stddev(c).over(w_rolling).alias(f"{col_name}_std_{window_name}")
            ])
            logger.info(f"Done calculating rolling statistic for {col_name} in {window_name}")
    logger.info('\n')

    log_step_headline(message="STEP 3.4: Extract advanced weather feature", border_char='-')
    logger.info("Calculating vapor pressure...")
    vapor_pressure = 6.112 * F.exp((17.67 * F.col("dew_point_2m")) / (F.col("dew_point_2m") + 243.5))
    logger.info("Calculating saturation vapor pressure")
    saturation_vapor_pressure = 6.112 * F.exp((17.67 * F.col("temperature_2m")) / (F.col("temperature_2m") + 243.5))
    logger.info("Calculating vapor pressure deficit...")
    vapor_pressure_deficit = saturation_vapor_pressure - vapor_pressure
    logger.info("Calculating apparent temperature...")
    apparent_temperature = F.col("temperature_2m") + 0.33 * vapor_pressure - 0.7 * F.col("wind_speed_10m") / KHM_TO_MS - 4.0
    feature_exprs.extend(
        [
            vapor_pressure.alias("vapor_pressure"),
            saturation_vapor_pressure.alias("saturation_vapor_pressure"),
            vapor_pressure_deficit.alias("vapor_pressure_deficit"),
            apparent_temperature.alias("apparent_temperature")
        ]
    )
    logger.info("Done extracting 4 advanced weather features!\n")

    gold_df = silver_df.select(*feature_exprs)

    # Take the largest rolling and lagging windows of temperature_2m as representation
    logger.info("Ensure valid rolling and lagging features..")
    gold_df = gold_df.dropna(subset=["temperature_2m_mean_1w", "temperature_2m_std_1w", "temperature_2m_lag_24hrs"])
    logger.info("SILVER -> GOLD TRANSFORMATION COMPLETED!\n")
    return gold_df

def save_gold_to_iceberg(spark: SparkSession, gold_df: pyspark.sql.DataFrame, target_table="lakehouse.gold.weather") -> None:
    """
    Writes the Gold DataFrame (Feature Store) to Apache Iceberg using Merge-Into (Upsert).
    
    1. Creates 'lakehouse.gold' namespace.
    2. Defines the massive Feature Schema (Base + Lags + Rolling + Advanced).
    3. Performs MERGE (Upsert) on (city, timestamp).
    """

    log_step_headline(message=f"STEP 4: SAVING GOLD DATA VERSION TO ICEBERG ({target_table})")

    # 1. Dynamically generate rolling feature SQL schema strings
    rolling_ddl_lines = []
    stats = ["mean", "std"]
    
    for feat in ROLLING_FEATURES:
        for size in WINDOW_SIZES:
            for stat in stats:
                col_name = f"{feat}_{stat}_{size}"
                rolling_ddl_lines.append(f"            {col_name} float,")
    
    # Join them into a single string block
    rolling_schema_block = "\n".join(rolling_ddl_lines)

    # 2. Ensure the table exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    ddl_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            -- [1] Base Dimensions & Silver Metrics
            city string,
            latitude double,
            longitude double,
            source string,
            timestamp timestamp,
            ingested_at timestamp,
            
            -- Base Weather (Direct from Silver)
            temperature_2m float,
            relative_humidity_2m float,
            dew_point_2m float,
            precipitation float,
            cloud_cover float,
            sunshine_duration float,
            wind_speed_10m float,
            wind_direction_10m float,
            weather_code float,
            soil_temperature_0_to_7cm float,
            soil_moisture_0_to_7cm float,

            -- [2] Cyclical Time Features
            hour_sin float,
            hour_cos float,
            day_of_year_sin float,
            day_of_year_cos float,

            -- [3] Lagged Features (Auto-regressive)
            temperature_2m_lag_1hrs float,
            temperature_2m_lag_2hrs float,
            temperature_2m_lag_3hrs float,
            temperature_2m_lag_24hrs float,
            dew_point_2m_lag_1hrs float,
            dew_point_2m_lag_2hrs float,
            dew_point_2m_lag_3hrs float,
            dew_point_2m_lag_24hrs float,

            -- [4] Advanced Physical Features
            vapor_pressure float,
            saturation_vapor_pressure float,
            vapor_pressure_deficit float,
            apparent_temperature float,

            -- [5] Rolling Statistics (Generated Dynamically)
{rolling_schema_block.rstrip(",")} -- Trim last comma
        )
        USING iceberg
        -- Partitioning for fast time-range queries
        PARTITIONED BY (city, days(timestamp))
    """
    
    spark.sql(ddl_query)
    logger.info(f"Table schema verified for {target_table}.")

    # 2. Sorting by city and timestamp for better compression & query speed.
    logger.info("Sorting Gold data by City and Timestamp...")
    sorted_df = gold_df.sort("city", "timestamp")

    # Create a temporary view so we can reference this batch in SQL
    sorted_df.createOrReplaceTempView("gold_incoming_batch")

    # 3. Merge logic: 
    # - Match on (city, timestamp) -> This defines the "Primary Key"
    # - If Match: UPDATE (overwrite with new cleaned data)
    # - If Not Match: INSERT (new record)
    logger.info("Executing MERGE INTO (Upsert) operation...")
    merge_query = f"""
        MERGE INTO {target_table} AS target
        USING gold_incoming_batch AS source
        ON target.city = source.city 
           AND target.timestamp = source.timestamp
        
        WHEN MATCHED THEN 
            UPDATE SET *
            
        WHEN NOT MATCHED THEN 
            INSERT *
    """
    
    spark.sql(merge_query)
    logger.info(f"BATCH WRITE COMPLETED! Features saved to {target_table}.\n")

def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_k8s_spark("SilverToGold")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    # Read Silver data directly from Iceberg
    log_step_headline(message="STEP 2: Read Silver data")
    silver_df = (
        spark.read
        .format("iceberg")
        .load("lakehouse.silver.weather")
    )
    num_records = silver_df.count()
    logger.info(f'Found {num_records} weather data records!')
    logger.info('SUCCESSFULLY READ SILVER DATA!\n')

    logger.info(f"Before Silver -> Gold transformation: {silver_df.count()} data records!")
    log_step_headline(message="STEP 3: Perform Silver -> Gold data transition")
    gold_df = silver_gold_transform(silver_df)
    gold_df.cache()
    logger.info(f"After Silver -> Gold transformation: {gold_df.count()} data records")

    save_gold_to_iceberg(gold_df=gold_df, spark=spark)

    spark.stop()

if __name__ == '__main__':
    main()