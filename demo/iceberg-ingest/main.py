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
    spark.sql("""CREATE NAMESPACE IF NOT EXISTS lakehouse.cleaned""")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cleaned.events (
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
    warehouse_path = os.getenv("CATALOG_WAREHOUSE", "s3a://warehouse/")
    catalog_uri = os.getenv("CATALOG_URI", "http://localhost:19120/api/v2")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    return (
        SparkSession.builder
            .appName("minio-to-iceberg")
        
            # Catalog configs
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "nessie")
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path)
            .config("spark.sql.catalog.lakehouse.uri", catalog_uri)
            .config("spark.sql.catalog.lakehouse.ref", "main") # Nessie catalog branch to work in
            .config("spark.sql.catalog.lakehouse.authentication.type", "NONE") # Nessie authentication type (NONE, BEARER, OAUTH2, AWS)
            .config("spark.sql.defaultCatalog", "lakehouse")
            # .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
                        org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
            
            # MinIO S3 configs (for s3a interface)
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .getOrCreate()
    )

spark = get_spark_session()
raw_data_schema = get_schema()

raw_zone_path = os.getenv("RAW_ZONE_PATH", "raw-zone/topics/events/") # <bucket name>/<topic name>

create_iceberg_table(spark)

raw_df = (
    spark.read
        .format("json")             # or parquet, csv, etc.
        .schema(raw_data_schema)    # define the schema manually
        .load(f"s3a://{raw_zone_path}/*") # for Spark to track subdirectories of partitioned files
)

# ops here for basic data cleaning

query = (
    raw_df.write
        .format("iceberg")
        # .outputMode("append")
        # .trigger(processingTime="1 minute") # write period
        .option("checkpointLocation", "s3a://warehouse/checkpoints/iceberg-ingest/")
        # .toTable("lakehouse.cleaned.events")
        .mode("append")
        # .saveAsTable("lakehouse.cleaned.events")
)
# raw_df.writeTo("lakehouse.cleaned.events")

try:
    # query.awaitTermination()
    query.saveAsTable("lakehouse.cleaned.events")
except KeyboardInterrupt:
    # query.stop()
    print("Cleaning data stopped.")
finally:
    # query.awaitTermination()
    spark.stop()