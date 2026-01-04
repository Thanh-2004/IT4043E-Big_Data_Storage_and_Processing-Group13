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
from schemas.spark.schema import kafka_raw_schema

import logging
# Setting up logging configurations
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

WAREHOUSE_PATH = os.getenv("CATALOG_WAREHOUSE", "s3a://warehouse/nessie")
RAW_ZONE_PATH = os.getenv("RAW_ZONE_PATH", "raw-zone/topics/events/") # <bucket name>/<topic name>
CATALOG_URI = os.getenv("CATALOG_URI", "http://nessie:19120/api/v2")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "http://minio-internal-service:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

PHYSICAL_LIMITS = [
    ("temperature_2m", -95.0, 65.0),         
    ("relative_humidity_2m", 0.0, 105.0),    
    ("wind_speed_10m", 0.0, 410.0),         
    ("wind_direction_10m", 0.0, 360.0),     
    ("cloud_cover", 0.0, 100.0),             
    ("soil_temperature_0_to_7cm", -50.0, 75.0), 
    ("soil_moisture_0_to_7cm", -0.05, 1.05), 
    ("precipitation", 0.0, 400.0),           
    ("dew_point_2m", -95.0, 50.0),        
    ("sunshine_duration", 0.0, 3600.0),       
    ("weather_code", 0.0, 99.0)               
]

def log_step_headline(message: str, border_char='=') -> None:
    logger.info(border_char * 70)
    logger.info(message)
    logger.info(border_char * 70)
    return None

def create_k8s_spark(app_name: str):
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            # Catalog configs
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "nessie")
            .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE_PATH)
            .config("spark.sql.catalog.lakehouse.uri", CATALOG_URI)
            .config("spark.sql.catalog.lakehouse.ref", "main") # Nessie catalog branch to work in
            .config("spark.sql.catalog.lakehouse.authentication.type", "NONE") # Nessie authentication type (NONE, BEARER, OAUTH2, AWS)
            .config("spark.sql.defaultCatalog", "lakehouse")
            
            .config("spark.jars.packages", ",".join(PACKAGES))
            .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
                        org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
            
            # MinIO S3 configs (for s3a interface)
            .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .getOrCreate()
        )
        return spark
    except Exception as e:
        logger.error("Fail to create SparkSession!")
        raise

def bronze_silver_transform(bronze_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # Deduplicate based on timestamp and city
    log_step_headline(message="STEP 3.1: Deduplicate data", border_char='-')
    tmp_df = bronze_df.dropDuplicates(subset=['timestamp', 'city'])
    tmp_df.cache()
    logger.info(f'After deduplication: {tmp_df.count()} data records\n')

    transform_exprs = []

    # Transformations
    log_step_headline(message="STEP 3.2: Casting TimestampType(), Standardize string, Nullify outliers, and Decompose wind directions", border_char='-')
    timestamp_cols = ["timestamp", "ingested_at"]
    logger.info(f'Found {len(timestamp_cols)} timestamp columns!')
    string_cols = {col for col, type in bronze_df.dtypes if (type == "string") and (col not in timestamp_cols)}
    logger.info(f'Found {len(string_cols)} string columns!')
    limit_dict = {col_name: (min_val, max_val) for col_name, min_val, max_val in PHYSICAL_LIMITS}
    wind_cols = {"wind_direction_10m"}
    
    for col_name in tmp_df.columns:
        c = F.col(col_name)
        # Convert StringType() -> TimestampType() for timestamp_cols
        if col_name in timestamp_cols:
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
        elif (col_name in limit_dict) and (col_name not in wind_cols):
            min_val, max_val = limit_dict[col_name]
            c = F.when((c < min_val) | (c > max_val), F.lit(None)).otherwise(c)
            logger.info(f"Done nullifying outliers for {col_name}: {min_val} < {col_name} < {max_val}.")
        # Nullify and decomponse wind directions
        elif col_name in wind_cols: 
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
    scalar_cols = [col for col in limit_dict.keys() if col not in wind_cols]
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
        if col_name in wind_cols:
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
    
    # Unpersist cached dataframe
    tmp_df.unpersist()
    
    logger.info("BRONZE -> SILVER TRANSFORMATION COMPLETED!\n")
    return silver_df

def silver_gold_transform(silver_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    feature_exprs = [F.col(col_name) for col_name in silver_df.columns]
    # Pre-compute used mathematical constants
    TWO_PI = 2 * math.pi
    HOURS_PER_DAY = 24.0
    DAYS_PER_YEAR = 365.25
    KHM_TO_MS = 3.6

    log_step_headline(message="STEP 4.1: Extract cyclical time encoding", border_char='-')
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

    log_step_headline(message="STEP 4.2: Process lagged features", border_char='-')
    lag_features = ["temperature_2m", "dew_point_2m"]
    available_lag_features = [feature for feature in lag_features if feature in silver_df.columns]
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

    log_step_headline(message="STEP 4.3: Discover data trends", border_char='-')
    rolling_features = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "wind_speed_10m",
        "precipitation"
    ]
    available_rolling_features = [feature for feature in rolling_features if feature in silver_df.columns]
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

    log_step_headline(message="STEP 4.4: Extract advanced weather feature", border_char='-')
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

def save_gold_to_iceberg(spark: SparkSession, gold_df: pyspark.sql.DataFrame, target_table="lakehouse.gold.weather") -> None:
    """
    Writes the Gold DataFrame (Feature Store) to Apache Iceberg using Merge-Into (Upsert).
    
    1. Creates 'lakehouse.gold' namespace.
    2. Defines the massive Feature Schema (Base + Lags + Rolling + Advanced).
    3. Performs MERGE (Upsert) on (city, timestamp).
    """

    log_step_headline(message=f"STEP 5: SAVING GOLD DATA VERSION TO ICEBERG ({target_table})")

    # 1. Dynamically generate rolling feature SQL schema strings
    rolling_ddl_lines = []
    rolling_feats = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "wind_speed_10m", "precipitation"]
    windows = ["12hrs", "1d", "3ds", "1w"]
    stats = ["mean", "std"]
    
    for feat in rolling_feats:
        for window in windows:
            for stat in stats:
                col_name = f"{feat}_{stat}_{window}"
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
    spark = create_k8s_spark("BatchProcessing")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    log_step_headline(message="STEP 2: Read Bronze (Raw) data")
    # HEADS-UP! CHANGE THIS TO READ THE CORRECT DATA OBJECT FROM MINIO
    # SAMPLE DATA TRIAL
    bronze_df = (
        spark.read
        .format("json")
        .schema(kafka_raw_schema)
        .option("recursiveFileLookup", "true")
        .load(f"s3a://{RAW_ZONE_PATH}")
    )
    num_records = bronze_df.count()
    logger.info(f'Found {num_records} weather data records!')
    logger.info('SUCCESSFULLY READ BRONZE DATA!\n')

    log_step_headline(message="STEP 3: Perform Bronze -> Silver data transition")
    silver_df = bronze_silver_transform(bronze_df)
    save_silver_to_iceberg(silver_df=silver_df, spark=spark)
    silver_df.cache()

    logger.info(f"Before Silver -> Gold transformation: {silver_df.count()} data records!")
    log_step_headline(message="STEP 4: Perform Gold -> Silver data transition")
    gold_df = silver_gold_transform(silver_df)
    save_gold_to_iceberg(gold_df=gold_df, spark=spark)
    gold_df.cache()
    logger.info(f"After Silver -> Gold transformation: {gold_df.count()} data records")
    spark.stop()

if __name__ == '__main__':
    main()