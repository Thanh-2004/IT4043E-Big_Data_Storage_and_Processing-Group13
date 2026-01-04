import os
import sys

# Get to the project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)

import logging
# Setting up logging configurations
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
)

WAREHOUSE_PATH = os.getenv("CATALOG_WAREHOUSE", "s3a://warehouse/nessie")
CATALOG_URI = os.getenv("CATALOG_URI", "http://nessie:19120/api/v2")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "http://minio-internal-service:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

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
            
            # MongoDB connection
            .config("spark.mongodb.write.connection.uri", "mongodb://mongo:mongo123@mongodb:27017")
            
            .getOrCreate()
        )
        return spark
    except Exception as e:
        logger.error("Fail to create SparkSession!")
        raise