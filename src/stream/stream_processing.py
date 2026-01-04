import os
import sys

# Get to the project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)

import yaml
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.utils.spark_action import create_k8s_spark
from src.schemas.spark.schema import kafka_raw_schema, raw_schema
from src.utils.data_constants import PHYSICAL_LIMITS, TIMESTAMP_COLS

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

def streaming_transform(streaming_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # We need the ALL columns to be of the correct type first before using deduplication with watermark
    transform_exprs = []

    log_step_headline(message="STEP 2.1: Casting TimestampType(), Standardize string, and Nullify outliers", border_char='-')
    
    available_timestamp_cols = {col for col in streaming_df.columns if col in TIMESTAMP_COLS}
    logger.info(f'Found {len(available_timestamp_cols)} timestamp columns: {available_timestamp_cols}')
    
    string_cols = {col for col, type in streaming_df.dtypes if (type == "string") and (col not in available_timestamp_cols)}
    logger.info(f'Found {len(string_cols)} string columns: {string_cols}')
    
    limit_dict = {col_name: (min_val, max_val) for col_name, min_val, max_val in PHYSICAL_LIMITS}
    
    for col_name in streaming_df.columns:
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

# Load MongoDB config globally
config_path = os.path.join(PROJECT_ROOT, "src", "config.yaml")
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

MONGODB_URI = config['mongodb']['uri']
MONGODB_DATABASE = config['mongodb']['database']
MONGODB_COLLECTION = config['mongodb']['collection']

# Import pymongo for direct MongoDB write
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

def write_to_mongodb(batch_df, batch_id):
    """Write batch DataFrame to MongoDB using pymongo (more stable than MongoDB Spark Connector)"""
    logger.info(f"[BATCH {batch_id}] Starting write to MongoDB using pymongo")
    
    # Convert Spark DataFrame to list of dicts
    records = batch_df.toPandas().to_dict('records')
    
    if not records:
        logger.info(f"[BATCH {batch_id}] No records to write")
        return
    
    # Write to MongoDB using pymongo
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Insert records
        result = collection.insert_many(records, ordered=False)
        logger.info(f"[BATCH {batch_id}] Successfully written {len(result.inserted_ids)} records to MongoDB")
        
    except BulkWriteError as bwe:
        # Some duplicates may exist - log but don't fail
        logger.warning(f"[BATCH {batch_id}] Bulk write error (likely duplicates): {bwe.details}")
    except Exception as e:
        logger.error(f"[BATCH {batch_id}] Error writing to MongoDB: {e}")
        raise
    finally:
        client.close()

def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_k8s_spark("StreamProcessing")
    logger.info('SparkSession SUCCESSFULLY CREATED!\n')

    log_step_headline(message="STEP 2: Read Raw data")

    # Đọc streaming data từ folder raw-data
    raw_data_path = os.path.join(PROJECT_ROOT, "raw-data")
    logger.info(f"Reading streaming data from: {raw_data_path}")
    
    raw_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(raw_schema)  
        .load(raw_data_path)
    )
    log_step_headline(message="STEP 3: Transform Stream Data")
    # Thêm cột ingested_at (thời điểm xử lý) trước khi transform
    raw_df_with_timestamp = raw_df.withColumn("ingested_at", F.current_timestamp())
    
    # Transform the streaming data using the defined function
    cleaned_df = streaming_transform(raw_df_with_timestamp)
    logger.info("Stream transformation completed!\n")

    log_step_headline(message="STEP 4: Write Stream to MongoDB")
    
    logger.info(f"MongoDB URI: {MONGODB_URI}")
    logger.info(f"Database: {MONGODB_DATABASE}")
    logger.info(f"Collection: {MONGODB_COLLECTION}")
    
    checkpoint_dir = os.path.join(PROJECT_ROOT, "checkpoints", "stream_to_mongo")
    logger.info(f"Checkpoint directory: {checkpoint_dir}")
    
    query = (
        cleaned_df.writeStream
        .foreachBatch(write_to_mongodb)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime='10 seconds')  # Xử lý mỗi 10 giây
        .start()
    )
    
    logger.info("Streaming query started! Running for 30 seconds...")
    
    # Run for 30 seconds for testing, then stop
    query.awaitTermination(timeout=30)
    query.stop()
    logger.info("Streaming query completed!") 


if __name__ == "__main__":
    main()