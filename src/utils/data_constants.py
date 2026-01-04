import math

# COLUMN-RELATED CONSTANTS
PHYSICAL_LIMITS = [
    ("temperature_2m", -95.0, 65.0),         
    ("relative_humidity_2m", 0.0, 105.0),    
    ("wind_speed_10m", 0.0, 410.0),         
    ("wind_direction_10m", 0.0, 360.0),     
    ("cloud_cover", 0.0, 100.0),             
    ("soil_temperature_0_to_7cm", -50.0, 75.0), 
    ("soil_moisture_0_to_7cm", -0.05, 1.05), 
    ("precipitation", 0.0, 400.0),           
    ("dew_point_2m", -95.0, 50.0),        
    ("sunshine_duration", 0.0, 3600.0),       
    ("weather_code", 0.0, 99.0)               
]

TIMESTAMP_COLS = ["timestamp", "ingested_at"]

WIND_DIRECTION_COLS = ["wind_direction_10m"]

# FEATURE-RELATED CONSTANTS
WINDOW_SIZES = ["12hrs", "1d", "3ds", "1w"]

ROLLING_FEATURES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "precipitation"
]

LAGGING_FEATURES = ["temperature_2m", "dew_point_2m"]

# MATHEMATICAL CONSTANTS
TWO_PI = 2 * math.pi
HOURS_PER_DAY = 24.0
DAYS_PER_YEAR = 365.25
KHM_TO_MS = 3.6