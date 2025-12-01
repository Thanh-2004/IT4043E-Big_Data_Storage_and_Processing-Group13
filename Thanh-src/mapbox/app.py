from flask import Flask, jsonify, send_from_directory
import random

app = Flask(__name__)

# --- Mô phỏng dữ liệu di chuyển ngẫu nhiên ---
def generate_taxi_data():
    taxis = []
    base_coords = [106.660172, 10.762622]  # trung tâm HCM
    for i in range(10):
        lng = base_coords[0] + random.uniform(-0.02, 0.02)
        lat = base_coords[1] + random.uniform(-0.02, 0.02)
        taxis.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": {"name": f"Taxi #{i+1}"}
        })
    return {"type": "FeatureCollection", "features": taxis}


@app.route("/")
def index():
    # Giao diện chính
    return send_from_directory("static", "map.html")


@app.route("/geojson")
def geojson():
    # API trả dữ liệu GeoJSON
    return jsonify(generate_taxi_data())


if __name__ == "__main__":
    app.run(debug=True, port=5000)
