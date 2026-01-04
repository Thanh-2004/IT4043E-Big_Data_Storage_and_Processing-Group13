# Historical Weather Data Documentation

This dataset contains hourly historical weather records fetched from the [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api). The data is intended for use in building and training machine learning models for weather forecasting (e.g., predicting temperature or precipitation).

## Data Source
* **Provider:** Open-Meteo
* **Timezone:** Asia/Bangkok (UTC+7)
* **Resolution:** Hourly

## Feature Definitions (Data Dictionary)

The dataset consists of the following 11 features. All numerical values are stored as `float32`.

| Feature Name | Unit | Description |
| :--- | :--- | :--- |
| **`temperature_2m`** | °C | **Air Temperature.** Measured at 2 meters above ground. This is the standard measurement height for meteorological observations. |
| **`relative_humidity_2m`** | % | **Relative Humidity.** Measured at 2 meters above ground. Indicates the amount of water vapor present in air expressed as a percentage of the amount needed for saturation at the same temperature. |
| **`dew_point_2m`** | °C | **Dew Point Temperature.** Measured at 2 meters above ground. The temperature the air needs to be cooled to (at constant pressure) in order to achieve a relative humidity (RH) of 100%. |
| **`precipitation`** | mm | **Total Precipitation.** The sum of rain, showers, and snow over the preceding hour. |
| **`cloud_cover`** | % | **Total Cloud Cover.** The percentage of the sky covered by clouds. 0% indicates a clear sky, while 100% indicates a completely overcast sky. |
| **`sunshine_duration`** | s | **Sunshine Duration.** The number of seconds of sunshine during the preceding hour. This is calculated based on direct solar irradiance exceeding 120 W/m². |
| **`wind_speed_10m`** | km/h | **Wind Speed.** Average wind speed measured at 10 meters above ground. |
| **`wind_direction_10m`** | ° | **Wind Direction.** Measured at 10 meters above ground in degrees (0° = North, 90° = East, 180° = South, 270° = West). Indicates the direction the wind is coming *from*. |
| **`weather_code`** | Numeric | **WMO Weather Code.** A numeric classification of the weather condition. (See Reference Table below). |
| **`soil_temperature_0_to_7cm`** | °C | **Soil Temperature.** Average temperature in the top soil layer (0-7 cm depth). |
| **`soil_moisture_0_to_7cm`** | m³/m³ | **Volumetric Soil Moisture.** Average water content in the top soil layer (0-7 cm depth). Defined as volume of water per volume of soil. |

---

## Weather Code Reference (WMO)

The `weather_code` feature uses the World Meteorological Organization (WMO) code system to categorize weather conditions:

| Code | Description |
| :--- | :--- |
| **0** | Clear sky |
| **1, 2, 3** | Mainly clear, partly cloudy, and overcast |
| **45, 48** | Fog and depositing rime fog |
| **51, 53, 55** | Drizzle: Light, moderate, and dense intensity |
| **56, 57** | Freezing Drizzle: Light and dense intensity |
| **61, 63, 65** | Rain: Slight, moderate and heavy intensity |
| **66, 67** | Freezing Rain: Light and heavy intensity |
| **71, 73, 75** | Snow fall: Slight, moderate, and heavy intensity |
| **77** | Snow grains |
| **80, 81, 82** | Rain showers: Slight, moderate, and violent |
| **85, 86** | Snow showers slight and heavy |
| **95** | Thunderstorm: Slight or moderate |
| **96, 99** | Thunderstorm with slight and heavy hail |

## Data Schema (PySpark / Pandas)

When loading this data, the schema corresponds to the following types:

```python
root
 |-- temperature_2m: float (nullable = true)
 |-- relative_humidity_2m: float (nullable = true)
 |-- dew_point_2m: float (nullable = true)
 |-- precipitation: float (nullable = true)
 |-- cloud_cover: float (nullable = true)
 |-- sunshine_duration: float (nullable = true)
 |-- wind_speed_10m: float (nullable = true)
 |-- wind_direction_10m: float (nullable = true)
 |-- weather_code: float (nullable = true)
 |-- soil_temperature_0_to_7cm: float (nullable = true)
 |-- soil_moisture_0_to_7cm: float (nullable = true)