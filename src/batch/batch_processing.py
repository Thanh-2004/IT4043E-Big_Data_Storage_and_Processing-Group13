import os
import sys

# Get to the project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)

from schemas.spark.schema import raw_schema
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import unicodedata
import re


import logging
# Setting up logging configurations
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

# Get S3 credentials securely (refer to k8s/secrets/s3-cred.yaml)
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

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
    # HEADS-UP! CHANGE THIS TO HELP SPARK RUN ON K8S
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", PACKAGES)
            # Set UTC timezone Spark behavior
            .config("spark.sql.session.timeZone", "UTC")
            # Iceberg Catalog Configuration (Hadoop/File-based on S3)
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", "s3a://lake/warehouse")
            # MinIO S3 Configuration (Cluster Network) ---
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio-internal-service.bigdata-pipeline:9000")
            # Credentials
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            # Required S3A Settings for MinIO
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
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

    # Transformations
    log_step_headline(message="STEP 3.2. Standardize string, Nullify outliers, and Decompose wind directions", border_char='-')
    string_cols = {col for col, type in bronze_df.dtypes if type == "string"}
    logger.info(f'Found {len(string_cols)} string columns!')
    limit_dict = {col_name: (min_val, max_val) for col_name, min_val, max_val in PHYSICAL_LIMITS}
    wind_cols = {"wind_direction_10m"}
    
    transform_exprs = []

    for col_name in tmp_df.columns:
        c = F.col(col_name)
        # Standardize string columns
        if col_name in string_cols:
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
    
    logger.info("BRONZE -> SILVER TRANSFORMATION COMPLETED!")
    return silver_df

def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_k8s_spark("BatchProcessing")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    log_step_headline(message="STEP 2: Read Bronze (Raw) data")
    # HEADS-UP! CHANGE THIS TO READ THE CORRECT DATA OBJECT FROM MINIO
    # SAMPLE DATA TRIAL
    bronze_df = (
        spark.read
        .option("header", True)
        .schema(raw_schema)
        .csv("raw_data/Hanoi_20000101_20251205.csv")
    )
    num_records = bronze_df.count()
    logger.info(f'Found {num_records} weather data records!')
    logger.info('SUCCESSFULLY READ BRONZE DATA!\n')

    log_step_headline(message="STEP 3: Perform Bronze -> Silver data transition")
    silver_df = bronze_silver_transform(bronze_df)

    # HEADS-UP: ADD THE CODE TO WRITE silver_df to Apache Iceberg here


if __name__ == '__main__':
    main()

