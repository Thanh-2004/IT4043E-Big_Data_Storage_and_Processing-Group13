from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofyear, month, sin, cos, radians
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("Weather Code Prediction ML") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

csv_path = "weather_history_2000-01-01_2025-12-05.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)
print(f"Total records: {df.count():,}")

df = df.withColumn("hour", hour(col("Date"))) \
       .withColumn("day_of_year", dayofyear(col("Date"))) \
       .withColumn("month", month(col("Date")))

df = df.withColumn("hour_sin", sin(radians(col("hour") * 360.0 / 24.0))) \
       .withColumn("hour_cos", cos(radians(col("hour") * 360.0 / 24.0))) \
       .withColumn("day_sin", sin(radians(col("day_of_year") * 360.0 / 365.0))) \
       .withColumn("day_cos", cos(radians(col("day_of_year") * 360.0 / 365.0))) \
       .withColumn("month_sin", sin(radians(col("month") * 360.0 / 12.0))) \
       .withColumn("month_cos", cos(radians(col("month") * 360.0 / 12.0)))

df = df.withColumnRenamed("Weather Code", "label") \
       .withColumnRenamed("Temperature (°C)", "temperature") \
       .withColumnRenamed("Precipitation (mm)", "precipitation") \
       .withColumnRenamed("Humidity (%)", "humidity") \
       .withColumnRenamed("Wind Speed (km/h)", "wind_speed")

df = df.select("label", "temperature", "precipitation", "humidity", "wind_speed",
               "hour_sin", "hour_cos", "day_sin", "day_cos", "month_sin", "month_cos")

print("Label Distribution:")
df.groupBy("label").count().orderBy("label").show()

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train_df.count():,} | Test: {test_df.count():,}")

feature_cols = ["temperature", "precipitation", "humidity", "wind_speed",
                "hour_sin", "hour_cos", "day_sin", "day_cos", "month_sin", "month_cos"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50, maxDepth=5, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, rf])
print(f"\nPipeline: VectorAssembler -> StandardScaler -> RandomForest(trees={rf.getNumTrees()}, depth={rf.getMaxDepth()})")

print("Training...")
model = pipeline.fit(train_df)
print("Training completed!")

predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
f1 = evaluator.setMetricName("f1").evaluate(predictions)

print(f"\nAccuracy: {accuracy:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f} | F1: {f1:.4f}")

print("\nConfusion Matrix:")
predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

rf_model = model.stages[-1]
feature_importances = rf_model.featureImportances.toArray()
importance_list = list(zip(feature_cols, feature_importances))
importance_list.sort(key=lambda x: x[1], reverse=True)

print("\nFeature Importance:")
for feature, importance in importance_list:
    bar = " " * int(importance * 100)
    print(f"{feature:<20} {importance:.4f} ({importance*100:5.2f}%)  {bar}")

print("\nAccuracy by Weather Code:")
for weather_code in [0, 1, 2, 3]:
    subset = predictions.filter(col("label") == weather_code)
    correct = subset.filter(col("label") == col("prediction")).count()
    total = subset.count()
    if total > 0:
        acc = correct / total
        print(f"  Code {weather_code}: {correct}/{total} = {acc:.4f} ({acc*100:.2f}%)")

model_path = "weather_code_model"
model.write().overwrite().save(model_path)
print(f"\nModel saved to: {model_path}")
print(f"Final Accuracy: {accuracy*100:.2f}%")

spark.stop()
