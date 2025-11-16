from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main():

    spark = SparkSession.builder \
        .appName("kafka-batch-to-iceberg-and-mongo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_cat.warehouse", "file:///opt/iceberg/warehouse") \
        .getOrCreate()

    kafka_bootstrap = "kafka:9092"
    kafka_topic = "weather_topic"

    weather_schema = StructType([
        StructField("station_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
    ])

    df_kafka = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), weather_schema).alias("data")) \
        .select("data.*") \
        .withColumnRenamed("timestamp", "event_time")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_cat.default")

    if not spark._jsparkSession.catalog().tableExists("hadoop_cat.default.weather_readings"):
        df_parsed.limit(0).writeTo("hadoop_cat.default.weather_readings").create()

    df_parsed.writeTo("hadoop_cat.default.weather_readings").append()

    df_parsed.write \
        .format("mongo") \
        .option("uri", "mongodb://root:example@mongo:27017/?authSource=admin") \
        .option("database", "weatherdb") \
        .option("collection", "readings") \
        .mode("append") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()
