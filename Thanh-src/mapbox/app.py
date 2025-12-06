import os
from flask import Flask, render_template
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    # Lấy token từ file .env truyền xuống HTML
    mapbox_token = os.getenv('MAPBOX_TOKEN')
    return render_template('index.html', mapbox_token=mapbox_token)

if __name__ == '__main__':
    app.run(debug=True, port=5000)