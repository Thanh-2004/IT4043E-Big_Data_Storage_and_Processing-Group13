## How to run

### 1. Build docker container
Run ```docker compose up --build -d``` to build docker container

### 2. Check Prometheus
Visit ```localhost:9090/targets``` to check Prometheus targets.
If 4 images are all up, then prometheus is running correctly.

### 3. Open Grafana
Visit ```localhost:3000``` to open Grafana.
Add Prometheus resource with URL: ```http://thanos-querier:10901``` or ```http://prometheus:9090``` if thanos is not running.

Add query, select metrics (e.g: weather_temperature_celcius), and Run queries. (for weather exporter)
To import system metrics dashboard, import dashboard with the code ```1860```. (node exporter included)


## Future work
- Fix Thanos
- Add more graphs
- Connect with internal mongoDB


