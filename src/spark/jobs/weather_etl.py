from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Define the Schema (Must match the OpenWeatherMap JSON exactly)
# We only keep the fields we actually care about to save space.
schema = StructType([
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", LongType())
    ])),
    StructField("weather", StringType()), # This comes as an array of objects in raw JSON
    StructField("name", StringType()),    # City Name
    StructField("dt", LongType())         # Timestamp
])

def start_etl():
    # 2. Initialize Spark with the MongoDB and Kafka Connectors
    spark = SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:password@mongo-service.default.svc.cluster.local:27017/weather_db.weather_collection?authSource=admin") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. Read from Kafka
    # We connect to the 'weather_data' topic we created earlier
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-service.default.svc.cluster.local:9092") \
        .option("subscribe", "weather_data") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Transform the Data
    # Kafka sends data as bytes in the 'value' column. We cast to String -> Parse JSON.
    weather_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select(
            col("data.name").alias("city"),
            col("data.main.temp").alias("temperature"),
            col("data.main.humidity").alias("humidity"),
            col("data.dt").alias("timestamp"),
            current_timestamp().alias("processed_at")
        )

    # 5. Write to MongoDB
    # We use 'append' mode to add new weather records as they arrive.
    query = weather_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/opt/spark/work/weather_checkpoint") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_etl()