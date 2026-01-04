# 🔄 Bronze-to-Silver Transformation Logic

The transition from the **Bronze Layer** (Raw Ingestion) to the **Silver Layer** (Refined Data) is the core data cleaning and quality assurance step in our pipeline. This process ensures that downstream ML models receive consistent, physically valid, and continuous time-series data.

The transformation pipeline follows a strict 4-step execution plan:

## 1. Deduplication
* **Logic:** Removes duplicate records based on the composite key of `['timestamp', 'city']`.
* **Purpose:** Ensures strict time-series integrity (one observation per hour per city).

## 2. Standardization & Outlier Nullification
Before imputation, data is cleaned to remove formatting errors and physically impossible values (Outliers).

### String Standardization
String columns (e.g., City Names) undergo the following normalization:
1.  **Lowercasing:** `Hanoi` -> `hanoi`
2.  **Accent Removal:** `đà nẵng` -> `da nang`
3.  **Special Char Removal:** Parentheses `()` are removed.
4.  **Slugification:** Non-alphanumeric characters are replaced with underscores (`_`).

### 🔬 Physical Limit Checks & Scientific References
Numerical weather data is validated against scientific bounds defined in `PHYSICAL_LIMITS`. Values outside these ranges are converted to `NULL` (treated as missing data) rather than being capped, to prevent introducing artificial bias.

| Feature | Valid Range | Unit | Scientific Basis & Reference |
| :--- | :--- | :--- | :--- |
| **`temperature_2m`** | -95.0 to 65.0 | °C | Covers Earth's historical extremes: Lowest (-89.2°C, Vostok) and Highest (56.7°C, Death Valley). <br>📚 *Ref: [WMO Archive of Weather and Climate Extremes](https://wmo.asu.edu/)* |
| **`relative_humidity_2m`** | 0.0 to 105.0 | % | While physically capped at 100%, capacitance sensors often drift or report supersaturation (fog). We allow a 5% margin for sensor calibration error. <br>📚 *Ref: Jensen, M. E., et al. (1990). "ASCE's Manual No. 70"* |
| **`wind_speed_10m`** | 0.0 to 410.0 | km/h | Upper bound covers the highest non-tornadic wind gust ever recorded (408 km/h during Cyclone Olivia). <br>📚 *Ref: Courtney, J., et al. (2012). "Documentation and verification of the world extreme wind gust record". Australian Met. Journal* |
| **`precipitation`** | 0.0 to 400.0 | mm | Safely covers the world record for 1-hour rainfall (~305mm in Holt, MO, 1947). <br>📚 *Ref: [WMO Archive of Weather and Climate Extremes](https://wmo.asu.edu/)* |
| **`soil_temperature`** | -50.0 to 75.0 | °C | Soil has high thermal inertia and rarely fluctuates as wildly as air temperature. However, desert surface soils can exceed 60°C. <br>📚 *Ref: Hillel, D. (1998). "Environmental Soil Physics". Academic Press* |
| **`soil_moisture`** | -0.05 to 1.05 | m³/m³ | Volumetric water content is ratio-based (0-1). We allow slight negatives (-0.05) to account for sensor noise in dry soil. <br>📚 *Ref: Vereecken, H., et al. (2008). "Pedotransfer functions to estimate the water retention curve"* |
| **`wind_direction_10m`** | 0.0 to 360.0 | ° | Standard meteorological compass degrees (0° = North, 90° = East). |
| **`cloud_cover`** | 0.0 to 100.0 | % | Physically bounded percentage of sky occlusion. |
| **`dew_point_2m`** | -95.0 to 50.0 | °C | Physically, Dew Point cannot exceed Air Temperature. The upper bound (50°C) is set near the theoretical maximum limit of atmospheric moisture capacity. |
| **`sunshine_duration`** | 0.0 to 3600.0 | s | Maximum physical duration of sunshine in one hour is 3600 seconds. |

### 🌬️ Special Handling: Wind Direction
**Wind Direction** (0-360°) presents a challenge for cleaning and imputation because it is circular (e.g., 359° is mathematically far from 1°, but physically adjacent).
* **Decomposition:** We convert the polar degree value into Cartesian vector components using trigonometry:
    * $x = \cos(\text{radians}(\text{direction}))$
    * $y = \sin(\text{radians}(\text{direction}))$
* These $x$ and $y$ components are treated as standard linear variables for the imputation step.

---

## 3. Advanced Imputation Strategy
We employ a **Hybrid Two-Stage Imputation** strategy to handle missing values (NULLs) caused by sensor failure or outlier removal.

### Stage 1: Weighted Moving Average (WMA)
This is the primary imputation method, designed to preserve local trends and smoothness. It looks at the **±3 hours** surrounding the missing value.

* **Window:** Partition by `city`, Order by `timestamp`.
* **Weights:** Closer neighbors have higher influence.
    * $t \pm 1$ hour: Weight **3.0**
    * $t \pm 2$ hours: Weight **2.0**
    * $t \pm 3$ hours: Weight **1.0**
* **Formula:**
    $$\text{Value} = \frac{\sum (\text{Neighbor Value} \times \text{Weight})}{\sum \text{Existing Weights}}$$

### Stage 2: Global Fill (Persistence Model Fallback)
If the WMA fails (e.g., a data gap > 7 hours where no neighbors exist), we use a fallback mechanism.
1.  **Forward Fill (Last Observation Carried Forward):** Assumes weather persists over time.
2.  **Backward Fill:** Used only for gaps at the very start of the dataset.
3.  **Default (-100.0):** Absolute last resort.

> **Optimization:** The window frames for Forward/Backward fill are strictly limited to **24 hours** (`rowsBetween(-24, -1)`). This limits the search space, transforming the operation complexity from $O(N^2)$ to $O(N)$, significantly reducing memory overhead on Spark executors.

---

## 4. Finalization
* **Vector Recomposition:** The imputed Wind $x$ and $y$ components are converted back to degrees using `atan2(y, x)` and normalized to the [0, 360) range.
* **Cleanup:** Temporary vector columns are dropped.
* **Result:** A clean, continuous, and scientifically valid dataset ready for the Apache Iceberg storage layer.

<br>

<hr style="border: 3px solid black;">

<br>

# 🔄 Silver-to-Gold Transformation Logic

The **Gold Layer** transformation takes the cleaned, continuous time-series data from the Silver Layer and enriches it with advanced features designed specifically for Machine Learning models (such as LSTMs or XGBoost) to forecast `temperature_2m`.

The transformation process involves four key steps:

## 1. Cyclical Time Encoding
Machine Learning models often struggle with raw cyclical features like "Hour of Day" (0-23) or "Day of Year" (1-365) because they interpret them linearly (e.g., 23 is mathematically far from 0, but physically adjacent).

To fix this, we project time features onto a unit circle using sine and cosine transformations:

$$x_{sin} = \sin\left(\frac{2\pi \cdot t}{\text{period}}\right), \quad x_{cos} = \cos\left(\frac{2\pi \cdot t}{\text{period}}\right)$$

* **Hour of Day:** Encoded as `hour_sin` and `hour_cos` (Period = 24.0)
* **Day of Year:** Encoded as `day_of_year_sin` and `day_of_year_cos` (Period = 365.25)

## 2. Autoregressive (Lag) Features
Weather forecasting relies heavily on the **Persistence Model**—the assumption that the future state of the atmosphere is strongly correlated with its recent past. We explicitly provide this historical context to the model.

We generate lag features for **Temperature** (`temperature_2m`) and **Dew Point** (`dew_point_2m`) at the following intervals:
* **1 hour, 2 hours, 3 hours:** To capture immediate short-term trends.
* **24 hours:** To capture the daily diurnal cycle (e.g., the temperature at 8 AM today is highly correlated with the temperature at 8 AM yesterday).

## 3. Rolling Statistics (Trend Discovery)
To help the model understand the "state" of the weather system (e.g., "Is the week getting warmer?"), we calculate rolling **Mean** and **Standard Deviation** for key variables.

**Target Variables:** `temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `wind_speed_10m`, `precipitation`.

**Time Windows:**
* **12 Hours (`_12hrs`):** Half-day trends (day/night transition).
* **24 Hours (`_1d`):** Daily trends.
* **72 Hours (`_3ds`):** Multi-day weather system movement.
* **1 Week (`_1w`):** Weekly seasonality and longer-term shifts.

## 4. Advanced Meteorological Features
Instead of arbitrary variable interactions, we derive scientifically validated indices based on atmospheric thermodynamics. These features expose complex physical relationships to the linear layers of the ML model.

| Feature | Formula / Logic | Scientific Basis & Reference |
| :--- | :--- | :--- |
| **Vapor Pressure ($e_a$)** | $6.112 \cdot \exp\left(\frac{17.67 \cdot T_{dew}}{T_{dew} + 243.5}\right)$ | Represents the partial pressure exerted by water vapor in the air. Unlike Relative Humidity, $e_a$ is an absolute measure of moisture independent of temperature changes. <br>📚 **Ref:** Bolton (1980), *"The Computation of Equivalent Potential Temperature"*. |
| **Saturation Vapor Pressure ($e_s$)** | $6.112 \cdot \exp\left(\frac{17.67 \cdot T_{air}}{T_{air} + 243.5}\right)$ | The maximum pressure of water vapor the air can hold at the current temperature before saturation (fog/rain) occurs. Based on the **Tetens Equation**. |
| **Vapor Pressure Deficit (VPD)** | $e_s - e_a$ | Measures the "drying power" of the air. A high VPD indicates dry air capable of high evaporation rates; a VPD near 0 indicates saturation (high probability of precipitation/fog). |
| **Apparent Temperature (AT)** | $T_a + 0.33e_a - 0.70V_{m/s} - 4.0$ | A combined index quantifying how the weather "feels." It mathematically integrates the warming effect of humidity (inhibiting sweat evaporation) and the cooling effect of wind (advection). <br>📚 **Ref:** Steadman (1984), *"A Universal Scale of Apparent Temperature"*. (Australian BOM approximation). |

> **Note on Data Trimming:** The generation of lagging and rolling features creates `NULL` values at the beginning of the dataset (the "burn-in" period). For example, a 1-week rolling average cannot be calculated for the first 168 hours of data. To ensure data quality, we drop the first week of records where these features are undefined.