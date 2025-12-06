mapboxgl.accessToken = MAPBOX_TOKEN;

const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/dark-v11',
    center: [106.5, 16.5],
    zoom: 5.5,
    projection: 'mercator'
});

map.on('load', () => {
    console.log("Map đã load xong!");
    
    // Gọi Module Heatmap
    // HeatmapManager.init(map);
    
    // Module Popup vẫn dùng được bình thường
    PopupManager.init(map);
    // // --> THÊM DÒNG NÀY <--
    // NasaManager.init(map);    // Module lịch sử mưa (NASA)

    HistoryManager.init(map); // <--- THÊM DÒNG NÀY
});