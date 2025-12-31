import random
import time
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AirflowRestTest").getOrCreate()
    
    # Simple CPU-heavy task to calculate Pi
    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    num_samples = 100000
    count = spark.sparkContext.parallelize(range(0, num_samples)) \
        .filter(inside).count()

    print(f"Pi is roughly {4.0 * count / num_samples}", flush=True)
    print("Sleeping")
    time.sleep(60)
    spark.stop()
    