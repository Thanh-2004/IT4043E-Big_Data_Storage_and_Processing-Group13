from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from functools import lru_cache
import os

@lru_cache
def get_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("value", DoubleType()),
        StructField("source", StringType()),
        StructField("created_at", StringType()) # timestamp
    ])

def create_iceberg_table(spark: SparkSession):
    spark.sql("""CREATE NAMESPACE IF NOT EXISTS my_catalog.demo""")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS my_catalog.demo.raw_events (
            id string,
            value double,
            source string,
            created_at string
        )
        USING iceberg
    """)
    # change created_at into timestamp later, right now saved in raw string format
    # add partitioning

def get_spark_session():
    catalog_uri = os.getenv("CATALOG_URI", "http://iceberg-rest:8181")
    return (
        SparkSession.builder
            .appName("minio-to-iceberg")
        
            # Catalog configs
            # simply need to config connection to catalog, other configs in the catalog's config (in container, ...)
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.my_catalog.type", "rest")
            .config("spark.sql.catalog.my_catalog.uri", catalog_uri)
            # .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")
            .config("spark.sql.catalog.my_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            # .config("spark.sql.catalog.my_catalog.default-namespace", "demo")
            .config("spark.sql.defaultCatalog", "my_catalog")
            
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            
            # MinIO S3 configs (for s3a interface)
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            
            .getOrCreate()
    )

spark = get_spark_session()
raw_data_schema = get_schema()

raw_zone_path = os.getenv("RAW_ZONE_PATH", "raw-zone/topics/events/") # <bucket name>/<topic name>

create_iceberg_table(spark)

raw_df = (
    spark.readStream
        .format("json")             # or parquet, csv, etc.
        .schema(raw_data_schema)    # define the schema manually
        .load(f"s3a://{raw_zone_path}/*") # for Spark to track subdirectories of partitioned files
)
query = (
    raw_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="1 minute") # write period
        .option("checkpointLocation", "s3a://checkpoints/iceberg-ingest/")
        .toTable("my_catalog.demo.raw_events")
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    print("Streaming to Iceberg stopped.")
finally:
    query.awaitTermination()