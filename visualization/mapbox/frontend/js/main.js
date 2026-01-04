import { Config } from './config.js';
import { fetchWeatherData } from './api.js';
import { WindSystem } from './wind.js';

// Khởi tạo Mapbox
mapboxgl.accessToken = Config.mapboxToken;
const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/dark-v10',
    center: [108.0, 16.0],
    zoom: 4.8
});

const windSystem = new WindSystem();
let globalData = {};
let sortedDates = [];
let currentStations = [];
let isPlaying = false;
let currentGlobalHour = 0;
let timer = null;

let currentMode = 'wind'; // Mặc định là gió

// Hàm xử lý dữ liệu thô
function processData(flatData) {
    globalData = {};
    const dateSet = new Set();
    flatData.forEach(item => {
        if (!globalData[item.date]) globalData[item.date] = [];
        globalData[item.date].push(item);
        dateSet.add(item.date);
    });
    sortedDates = Array.from(dateSet).sort();
}

// Hàm lấy dữ liệu trạm tại giờ cụ thể
function getStationData(hourIndex, dateKey) {
    const dayRecords = globalData[dateKey];
    if (!dayRecords) return [];

    const stations = [];
    dayRecords.forEach(rec => {
        // Gió
        const u = (rec.hourly_wind_u && rec.hourly_wind_u[hourIndex]) || 0;
        const v = (rec.hourly_wind_v && rec.hourly_wind_v[hourIndex]) || 0;
        const speed = Math.sqrt(u*u + v*v);
        let bearing = (Math.atan2(u, v) * 180 / Math.PI);
        if (bearing < 0) bearing += 360;

        // Các chỉ số khác
        const temp = (rec.hourly_temps && rec.hourly_temps[hourIndex]) || 0;
        const rain = (rec.hourly_rain && rec.hourly_rain[hourIndex]) || 0;
        const cloud = (rec.hourly_clouds && rec.hourly_clouds[hourIndex]) || 0;

        stations.push({
            coord: [rec.location.lon, rec.location.lat],
            speed: speed,
            bearing: bearing,
            temp: temp,   // <--- Mới
            rain: rain,   // <--- Mới
            cloud: cloud  // <--- Mới
        });
    });
    return stations;
}

function updateLayerStyle() {
    // 1. Cập nhật màu sắc cho các vòng tròn (Halo)
    if (map.getLayer('station-heat-halo')) {
        map.setPaintProperty('station-heat-halo', 'circle-color', Config.colors[currentMode]);
    }

    // 2. Ẩn/Hiện đường gió (Chỉ hiện khi mode == 'wind')
    if (map.getLayer('wind-lines-draw')) {
        const visibility = (currentMode === 'wind') ? 'visible' : 'none';
        map.setLayoutProperty('wind-lines-draw', 'visibility', visibility);
    }
}

function updateLegendUI() {
    const config = Config.legends[currentMode];
    if (!config) return;

    // Hiển thị box (lúc đầu nó ẩn)
    document.getElementById('legend-box').style.display = 'block';

    // Cập nhật nội dung
    document.getElementById('legend-title').innerText = config.title;
    document.getElementById('legend-bar').style.background = config.background;
    document.getElementById('legend-min').innerText = config.min;
    document.getElementById('legend-max').innerText = config.max;
}

// Cập nhật khung hình
async function updateFrame(globalHour) {
    currentGlobalHour = globalHour;
    const dayIdx = Math.floor(globalHour / 24);
    const hourIdx = globalHour % 24;
    const dateStr = sortedDates[dayIdx];
    
    document.getElementById('clock-time').innerText = `${hourIdx}:00`;
    document.getElementById('clock-date').innerText = dateStr;
    document.getElementById('hour-slider').value = globalHour;

    currentStations = getStationData(hourIdx, dateStr);
    
    // Update nền màu
    const stationGeoJSON = {
        type: 'FeatureCollection',
        features: currentStations.map(s => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: s.coord },
            properties: { 
                speed: s.speed,
                temp: s.temp,
                rain: s.rain,
                cloud: s.cloud
            }
        }))
    };
    const source = map.getSource('station-heat-source');
    if (source) source.setData(stationGeoJSON);

    // Reset đường gió với dữ liệu trạm mới
    if (currentMode === 'wind') {
        windSystem.initLines(currentStations);
    }
}

// Vòng lặp Animation
function animate() {
    if (!isPlaying) return;
    
    const geoJsonData = windSystem.computeNextFrame(map.getZoom());
    const source = map.getSource('wind-lines');
    if (source) source.setData(geoJsonData);

    requestAnimationFrame(animate);
}

// Gắn sự kiện cho nút
window.App = {
    loadData: async function() {
        const btn = document.getElementById('btn-load');
        const loading = document.getElementById('loading');
        btn.disabled = true; btn.innerText = "Đang đồng bộ...";
        loading.style.display = 'block';

        try {
            const data = await fetchWeatherData();
            if (!data) {
                throw new Error("Dữ liệu trả về là rỗng (undefined)");
            }
            if (data.error) {
                throw new Error(data.error);
            }
            // Chỉ gọi .length khi chắc chắn data là Mảng
            if (Array.isArray(data) && data.length === 0) {
                 throw new Error("Không có dữ liệu trong Database");
            }
            if (!data || data.length === 0) throw new Error("Không có dữ liệu");

            processData(data);
            
            const slider = document.getElementById('hour-slider');
            slider.max = sortedDates.length * 24 - 1;
            slider.value = 0;
            
            await updateFrame(0);
            updateLegendUI();

            btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-check"></i> Đã tải xong';
            loading.style.display = 'none';
        } catch (err) {
            console.error(err);
            alert("Lỗi: " + err.message);
            btn.disabled = false; btn.innerText = "Thử lại";
            loading.style.display = 'none';
        }
    },

    togglePlay: function() {
        if (isPlaying) {
            isPlaying = false;
            document.getElementById('btn-play').innerHTML = '<i class="fa-solid fa-play"></i> Chạy Diễn biến';
            clearInterval(timer);
        } else {
            if (windSystem.linesArr.length === 0) { alert("Vui lòng tải dữ liệu trước"); return; }
            isPlaying = true;
            document.getElementById('btn-play').innerHTML = '<i class="fa-solid fa-pause"></i> Dừng';
            animate();

            timer = setInterval(async () => {
                currentGlobalHour++;
                if (currentGlobalHour >= sortedDates.length * 24) currentGlobalHour = 0;
                await updateFrame(currentGlobalHour);
            }, 4000); 
        }
    },
    switchTab: function(mode) {
        currentMode = mode;
        
        // Cập nhật UI nút bấm
        document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
        // Tìm nút tương ứng để active (cách đơn giản)
        const btnId = `tab-${mode}`;
        const btn = document.getElementById(btnId);
        if(btn) btn.classList.add('active');

        // Cập nhật bản đồ
        updateLayerStyle();

        updateLegendUI();
        
        // Vẽ lại khung hình hiện tại với chế độ mới
        updateFrame(currentGlobalHour);
    }
};

// Khởi tạo Map
map.on('load', () => {
    // Layer nền
    map.addSource('station-heat-source', { type: 'geojson', data: {type: 'FeatureCollection', features: []} });
    map.addLayer({
        id: 'station-heat-halo',
        type: 'circle',
        source: 'station-heat-source',
        paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 25, 8, 80],
            'circle-blur': 0.8,
            'circle-opacity': 0.6,
            'circle-color': [
                'interpolate', ['linear'], ['get', 'speed'],
                0, '#2196f3', 15, '#00e676', 30, '#ffeb3b', 45, '#ff9800', 60, '#f44336', 100, '#d50000'
            ]
        }
    });

    // Layer đường gió
    map.addSource('wind-lines', { type: 'geojson', data: turf.featureCollection([]) });
    map.addLayer({
        id: 'wind-lines-draw',
        type: 'line',
        source: 'wind-lines',
        layout: { 
            'line-join': 'round', 
            'line-cap': 'round',
            'visibility': 'visible' // Mặc định hiện
        },
        paint: {
            'line-color': 'rgba(255, 255, 255, 0.85)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2.5, 10, 1.5]
        }
    });

    document.getElementById('hour-slider').addEventListener('input', (e) => {
        isPlaying = false;
        clearInterval(timer);
        document.getElementById('btn-play').innerHTML = '<i class="fa-solid fa-play"></i> Chạy Diễn biến';
        updateFrame(parseInt(e.target.value));
    });
});