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
from src.utils.spark_action import create_k8s_spark
from src.schemas.spark.schema import kafka_raw_schema
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

def main():
    log_step_headline(message="STEP 1: Create SparkSession")
    spark = create_k8s_spark("StreamProcessing")
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