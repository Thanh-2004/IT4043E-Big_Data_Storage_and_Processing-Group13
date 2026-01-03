##  Key Features

* **Visualizations:**
    *  **Wind / Temp / Rain / Cloud** 
* **Smart Data Sync:**
    * Automatically finds and downloads missing data.
    * **Retry Logic:** Automatically pauses and retries if the API is busy (Error 429).
* **Interactive UI:**
    * Time Slider to see changes over time.
    * Tabs to switch between Wind, Temperature, Rain, and Clouds.
    * Automatic Color Legends.

---

## 🛠️ Requirements

Before you start, make sure you have:

1.  **Node.js** (Version 16 or higher).
2.  **MongoDB** running on your computer.
    * *Recommendation:* Use Docker: `docker run -d -p 27017:27017 --name mongodb mongo`

---

## 📦 Installation

### 1. Folder Structure
Make sure your project folder looks like this:
```text
weather-app/
├── backend/            # Server code
├── frontend/           # Website code
├── package.json        # Library list
└── README.md

```

### 2. Install Libraries
Open your terminal in the project folder and run:

```
npm install
```


Configuration
To make the map work, you need a Mapbox Access Token.
Open the file frontend/js/config.js.
Replace the value of mapboxToken with your token.

```
export const Config = {
    mapboxToken: 'YOUR_MAPBOX_ACCESS_TOKEN_HERE',
    // ...
};
```


### 3.How to Run

#### Step 1: Start the Backend (Server)
This handles data and database connections.
Open a terminal.
Run this command:
```
node backend/server.js
```


#### Step 2: Start the Frontend (Website)
Important: Do not double-click index.html. You must use a local web server because this project uses JavaScript Modules.

Using VS Code (Recommended):
1. Install the Live Server extension.
2. Right-click on frontend/index.html.
3. Select "Open with Live Server".

### 4. Run with Docker

Run this command to build docker container:
```
docker compose up --build -d
```

And run ```docker compose down``` when finish.