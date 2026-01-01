from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, StringType

raw_schema = StructType(
    [
        StructField('timestamp', TimestampType(), True),
        StructField('temperature_2m', FloatType(), True),
        StructField('relative_humidity_2m', FloatType(), True),
        StructField('dew_point_2m', FloatType(), True),
        StructField('precipitation', FloatType(), True),
        StructField('cloud_cover', FloatType(), True),
        StructField('sunshine_duration', FloatType(), True),
        StructField('wind_speed_10m', FloatType(), True),
        StructField('wind_direction_10m', FloatType(), True),
        StructField('weather_code', FloatType(), True),
        StructField('soil_temperature_0_to_7cm', FloatType(), True),
        StructField('soil_moisture_0_to_7cm', FloatType(), True),
        StructField('city', StringType(), True)
    ]
)