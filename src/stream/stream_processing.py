import os
import sys

# Get to the project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from schemas.spark.schema import kafka_raw_schema
from src.batch.batch_processing_demo import log_step_headline, PHYSICAL_LIMITS

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

def create_streaming_spark(app_name):
    # ADD YOUR CODE HERE
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

def streaming_transform(streaming_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # We need the ALL columns to be of the correct type first before using deduplication with watermark
    transform_exprs = []

    log_step_headline(message="STEP 2.1: Casting TimestampType(), Standardize string, and Nullify outliers", border_char='-')
    
    timestamp_cols = {"timestamp", "ingested_at"}
    
    available_timestamp_cols = {col for col in streaming_df.columns if col in timestamp_cols}
    logger.info(f'Found {len(available_timestamp_cols)} timestamp columns: {available_timestamp_cols}')
    
    string_cols = {col for col, type in streaming_df.dtypes if (type == "string") and (col not in timestamp_cols)}
    logger.info(f'Found {len(string_cols)} string columns: {string_cols}')
    
    limit_dict = {col_name: (min_val, max_val) for col_name, min_val, max_val in PHYSICAL_LIMITS}
    
    for col_name in streaming_df.columns:
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
        elif col_name in limit_dict:
            min_val, max_val = limit_dict[col_name]
            c = F.when((c < min_val) | (c > max_val), F.lit(None)).otherwise(c)
            logger.info(f"Done nullifying outliers for {col_name}: {min_val} < {col_name} < {max_val}.") 
        # Add transformation
        transform_exprs.append(c.alias(col_name))

    transformed_df = streaming_df.select(*transform_exprs)

    # Deduplication with Watermark
    log_step_headline(message="STEP 2.2: Deduplication", border_char='-')
    cleaned_df = (
        transformed_df
        .withWatermark("ingested_at", "5 minutes")
        .dropDuplicates(subset=["city", "timestamp"])
    )
    return cleaned_df

def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_streaming_spark("StreamProcessing")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    log_step_headline(message="STEP 2: Read Raw data")
    # ADD CODE

    log_step_headline(message="STEP 3: Perform stream data processing")
    # use the method streaming_transform()
    # ADD CODE

    log_step_headline(message="STEP 4: Write Stream to MongoDB")
    # ADD CODE 


if __name__ == "__main__":
    main()