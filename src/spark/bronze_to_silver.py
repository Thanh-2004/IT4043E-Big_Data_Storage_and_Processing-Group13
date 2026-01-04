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
from schemas.schema import kafka_raw_schema
from utils.data_constants import PHYSICAL_LIMITS, TIMESTAMP_COLS, WIND_DIRECTION_COLS
from utils.spark_action import create_k8s_spark

RAW_ZONE = os.getenv("RAW_ZONE_PATH", "raw-zone/topics/openmeteo-data/") # <bucket name>/<topic name>

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

def bronze_silver_transform(bronze_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # Deduplicate based on timestamp and city
    log_step_headline(message="STEP 3.1: Deduplicate data", border_char='-')
    tmp_df = bronze_df.dropDuplicates(subset=['timestamp', 'city'])
    logger.info(f'After deduplication: {tmp_df.count()} data records\n')

    transform_exprs = []

    # Transformations
    log_step_headline(message="STEP 3.2: Casting TimestampType(), Standardize string, Nullify outliers, and Decompose wind directions", border_char='-')

    available_timestamp_cols = {col for col in tmp_df.columns if col in TIMESTAMP_COLS}
    logger.info(f'Found {len(available_timestamp_cols)} timestamp columns: {available_timestamp_cols}')
    
    string_cols = {col for col, type in tmp_df.dtypes if (type == "string") and (col not in available_timestamp_cols)}
    logger.info(f'Found {len(string_cols)} string columns: {string_cols}')

    available_wind_direction_cols = {col for col in tmp_df.columns if col in WIND_DIRECTION_COLS}
    logger.info(f'Found {len(available_wind_direction_cols)} wind direction columns: {available_wind_direction_cols}')

    limit_dict = {col_name: (min_val, max_val) for col_name, min_val, max_val in PHYSICAL_LIMITS}
    
    for col_name in tmp_df.columns:
        c = F.col(col_name)
        # Convert StringType() -> TimestampType() for available_timestamp_cols
        if col_name in available_timestamp_cols:
            c = c.cast("timestamp")
            logger.info(f"Done converting {col_name} from StringType() to TimestampType()")
        # Standardize string columns
        elif col_name in string_cols:
            c = (
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.translate(
                                F.regexp_replace(F.lower(c), "đ", "d"),
                                "()", ""
                            ),
                            r"[^a-z0-9]+", "_"
                        ),
                        r"^_+|_+$", ""
                    )
                )
            )
            logger.info(f"Done standardizing string column {col_name}.")
        # Nullify outlier based on real-world extreme weather data
        elif (col_name in limit_dict) and (col_name not in available_wind_direction_cols):
            min_val, max_val = limit_dict[col_name]
            c = F.when((c < min_val) | (c > max_val), F.lit(None)).otherwise(c)
            logger.info(f"Done nullifying outliers for {col_name}: {min_val} < {col_name} < {max_val}.")
        # Nullify and decomponse wind directions
        elif col_name in available_wind_direction_cols: 
            # Nullify
            c_clean = F.when((c < min_val) | (c > max_val), F.lit(None)).otherwise(c)
            # Add cleaned wind column
            transform_exprs.append(c_clean.alias(col_name))
            # Decompose to Cartesian components for circular interpolation
            rads = F.radians(c_clean)
            transform_exprs.append(F.cos(rads).alias(f"{col_name}_x"))
            transform_exprs.append(F.sin(rads).alias(f"{col_name}_y"))
            logger.info(f"Done nullifying outliers and decomposing wind direction column {col_name}.")
            continue 
        # Add transformation
        transform_exprs.append(c.alias(col_name))
    tmp_df = tmp_df.select(*transform_exprs)
    logger.info("DATA TRANSFORMATION COMPLETED!\n")

    # Weighted Moving Average imputation
    log_step_headline(message="STEP 3.3: Impute with Weighted Moving Average (WMA)", border_char='-')
    w_spec = Window.partitionBy("city").orderBy("timestamp") 
    # Forward-fill = Look back 24 hours for a non-NULL value (if WMA failed) 
    w_ff = Window.partitionBy("city").orderBy("timestamp").rowsBetween(-24, -1)
    # Backward-fill = Look forward 24 hours for a non-NULL value (if WMA failed)
    w_bf = Window.partitionBy("city").orderBy("timestamp").rowsBetween(1, 24)
    # Final resolution (no data found)
    default_val = F.lit(-100.0)
    
    # Identify columns needing imputation
    scalar_cols = [col for col in limit_dict.keys() if col not in available_wind_direction_cols]
    vector_cols = [col for col in tmp_df.columns if col.endswith(("_x", "_y"))]
    impute_cols = scalar_cols + vector_cols
    
    imputation_exprs = []

    for col_name in tmp_df.columns:
        # Pass through non-imputable columns
        if col_name not in impute_cols:
            imputation_exprs.append(F.col(col_name))
            continue

        # Take the last three and the next three hours weather data
        prev3 = F.lag(col_name, 3).over(w_spec)
        prev2 = F.lag(col_name, 2).over(w_spec)
        prev1 = F.lag(col_name, 1).over(w_spec)
        next1 = F.lead(col_name, 1).over(w_spec)
        next2 = F.lead(col_name, 2).over(w_spec)
        next3 = F.lead(col_name, 3).over(w_spec)

        # Handles edge cases: weights auto-zero for missing neighbors
        weighted_sum = (
            F.coalesce(prev3, F.lit(0.0)) * 1.0 +
            F.coalesce(prev2, F.lit(0.0)) * 2.0 +
            F.coalesce(prev1, F.lit(0.0)) * 3.0 +
            F.coalesce(next1, F.lit(0.0)) * 3.0 +
            F.coalesce(next2, F.lit(0.0)) * 2.0 +
            F.coalesce(next3, F.lit(0.0)) * 1.0 
        )
        
        total_weight = (
            F.when(prev3.isNotNull(), 1.0).otherwise(0.0) +
            F.when(prev2.isNotNull(), 2.0).otherwise(0.0) +
            F.when(prev1.isNotNull(), 3.0).otherwise(0.0) +
            F.when(next1.isNotNull(), 3.0).otherwise(0.0) +
            F.when(next2.isNotNull(), 2.0).otherwise(0.0) +
            F.when(next3.isNotNull(), 1.0).otherwise(0.0)
        )

        # WMA = weighted_sum / total_weight (null if no neighbors exist)
        wma = F.when(total_weight > 0, weighted_sum / total_weight).otherwise(F.lit(None))
        # Forward-fill
        ff = F.last(col_name, ignorenulls=True).over(w_ff)
        # Backward-fill
        bf = F.first(col_name, ignorenulls=True).over(w_bf)
        
        # Fill nulls with WMA
        imputation_exprs.append(
            F.coalesce(
                F.col(col_name),
                wma, ff, bf, default_val
            ).alias(col_name)
        )
        logger.info(f"Done imputing data for column {col_name}.")

    tmp_df = tmp_df.select(*imputation_exprs)
    logger.info("DATA IMPUTATION COMPLETED!\n")

    # Recompose wind directions
    log_step_headline(message="STEP 3.4: Recompose Wind Direction from Vectors", border_char='-')
    
    final_exprs = []
    processed_wind_cols = set()
    
    for col_name in tmp_df.columns:
        # Recompose wind direction from interpolated x,y components
        if col_name in available_wind_direction_cols:
            x_col = f"{col_name}_x"
            y_col = f"{col_name}_y"
            # Convert Cartesian back to polar (degrees)
            # atan2(y,x) returns radians, convert to 0-360° range
            angle_rad = F.atan2(F.col(y_col), F.col(x_col))
            angle_deg = F.degrees(angle_rad)
            
            # Normalize to [0, 360) range
            normalized = (angle_deg + 360.0) % 360.0
            
            final_exprs.append(normalized.alias(col_name))
            processed_wind_cols.add(x_col)
            processed_wind_cols.add(y_col)
            logger.info(f"Done vector recomposing for wind direction column {col_name}")
        # Drop temporary vector components
        elif col_name in processed_wind_cols:
            continue
        # Keep all other columns
        else:
            final_exprs.append(F.col(col_name))
        

    silver_df = tmp_df.select(*final_exprs)

    return silver_df

def save_silver_to_iceberg(spark: SparkSession, silver_df: pyspark.sql.DataFrame, target_table="lakehouse.silver.weather") -> None:
    """
    Writes the Silver DataFrame to Apache Iceberg using a Batch Merge (Upsert) strategy.
    
    1. Creates the table if it doesn't exist (Idempotent).
    2. Sorts data for optimal storage clustering.
    3. Performs a MERGE INTO to update existing records or insert new ones.
    """
    
    log_step_headline(message=f"STEP 4: SAVING SILVER DATA VERSION TO ICEBERG ({target_table})")

    # 1. Ensure the table exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    ddl_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            -- Location / Metadata
            city string,
            latitude double,
            longitude double,
            source string,
            
            -- Time (Crucial for Partitioning & Ordering)
            timestamp timestamp,
            ingested_at timestamp,
            
            -- Weather Features (Payload)
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
            soil_moisture_0_to_7cm float
        )
        USING iceberg
        -- PARTITIONING STRATEGY:
        -- 1. 'city': Keeps data for each location physically together.
        -- 2. 'days(timestamp)': Hidden Partitioning (Daily buckets).
        PARTITIONED BY (city, days(timestamp))
    """
    spark.sql(ddl_query)
    logger.info(f"Table schema verified for {target_table}.")


    # 2. Sorting by city and timestamp for better compression & query speed.
    logger.info("Sorting Silver data by City and Timestamp for optimal file layout...")
    sorted_df = silver_df.sort("city", "timestamp")
    
    # Create a temporary view so we can reference this batch in SQL
    sorted_df.createOrReplaceTempView("batch_source_data")

    # 3. Merge logic: 
    # - Match on (city, timestamp) -> This defines the "Primary Key"
    # - If Match: UPDATE (overwrite with new cleaned data)
    # - If Not Match: INSERT (new record)
    
    logger.info("Executing MERGE INTO (Upsert) operation...")
    
    merge_query = f"""
        MERGE INTO {target_table} AS target
        USING batch_source_data AS source
        ON target.city = source.city 
           AND target.timestamp = source.timestamp
        
        -- Case 1: The record exists (e.g., we are re-processing old data to fix bugs)
        WHEN MATCHED THEN 
            UPDATE SET *
            
        -- Case 2: The record is new (standard ingestion)
        WHEN NOT MATCHED THEN 
            INSERT *
    """
    
    spark.sql(merge_query)
    
    logger.info(f"BATCH WRITE COMPLETED! Data saved to {target_table}.\n")

    return None


def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_k8s_spark("BronzeToSilver")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    log_step_headline(message="STEP 2: Read Bronze (Raw) data")
    # HEADS-UP! CHANGE THIS TO READ THE CORRECT DATA OBJECT FROM MINIO
    # SAMPLE DATA TRIAL
    bronze_df = (
        spark.read
        .format("json")
        .schema(kafka_raw_schema)
        .option("recursiveFileLookup", "true")
        .load(f"s3a://{RAW_ZONE}")
    )
    num_records = bronze_df.count()
    logger.info(f'Found {num_records} weather data records!')
    logger.info('SUCCESSFULLY READ BRONZE DATA!\n')

    log_step_headline(message="STEP 3: Perform Bronze -> Silver data transition")
    silver_df = bronze_silver_transform(bronze_df)
    logger.info("BRONZE -> SILVER TRANSFORMATION COMPLETED!\n")
    silver_df.cache()
    logger.info(f"Before Silver -> Gold transformation: {silver_df.count()} data records!")
    
    save_silver_to_iceberg(silver_df=silver_df, spark=spark)

    spark.stop()

if __name__ == '__main__':
    main()