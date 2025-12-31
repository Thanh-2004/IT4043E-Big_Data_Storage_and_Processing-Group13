from pyspark.sql import SparkSession

def test_connection():
    # Setup Spark with the Mongo Connector
    spark = SparkSession.builder \
        .appName("TestMongo") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:password@mongo-service.default.svc.cluster.local:27017/weather_db.test_collection?authSource=admin") \
        .getOrCreate()

    # Create dummy data
    print("--- GENERATING TEST DATA ---")
    data = [("Connection_Test_City", 99.9)]
    columns = ["city", "temperature"]
    df = spark.createDataFrame(data, columns)

    # Attempt to write
    print("--- ATTEMPTING WRITE TO MONGO ---")
    try:
        df.write \
          .format("mongodb") \
          .mode("append") \
          .save()
        print(">>> SUCCESS: Successfully wrote to MongoDB! <<<")
    except Exception as e:
        print(">>> FAILURE: Could not write to MongoDB. <<<")
        # This will print the REAL error (Auth vs Network)
        print(e)

    spark.stop()

if __name__ == "__main__":
    test_connection()