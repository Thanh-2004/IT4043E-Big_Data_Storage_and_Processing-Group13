const HistoryManager = {
    map: null,
    dataCache: [], // Chứa dữ liệu nhiệt độ của 24 giờ
    currentHour: 0,
    timer: null,
    isPlaying: false,

    init: function(mapInstance) {
        this.map = mapInstance;
        this.setupControls();
        
        // Mặc định chọn ngày hôm qua
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        document.getElementById('history-date').value = yesterday.toISOString().split('T')[0];
    },

    fetchHistoryData: function(dateStr) {
        // 1. Dừng animation cũ
        this.stopAnimation();
        document.getElementById('history-time-display').innerText = "Đang tải dữ liệu lịch sử...";

        // 2. Chuẩn bị URL (Archive API)
        // Lưu ý: Dùng Grid điểm từ file config.js (VIETNAM_GRID)
        const lats = VIETNAM_GRID.map(p => p.lat).join(',');
        const lons = VIETNAM_GRID.map(p => p.lon).join(',');

        const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${lats}&longitude=${lons}&start_date=${dateStr}&end_date=${dateStr}&hourly=temperature_2m&timezone=auto`;

        fetch(url)
            .then(res => res.json())
            .then(data => {
                // data có thể là mảng (nếu nhiều điểm) hoặc object (nếu 1 điểm)
                if (!Array.isArray(data)) data = [data];
                
                this.processData(data);
                document.getElementById('history-time-display').innerText = `Đã tải xong ngày ${dateStr}`;
            })
            .catch(err => {
                console.error(err);
                document.getElementById('history-time-display').innerText = "Lỗi tải dữ liệu!";
            });
    },

    processData: function(apiResponse) {
        // API trả về: Mảng các địa điểm. Mỗi địa điểm chứa mảng 24 giờ nhiệt độ.
        // Ta cần đảo ngược: Tạo mảng 24 giờ. Mỗi giờ chứa nhiệt độ của tất cả địa điểm.
        
        this.dataCache = []; // Reset cache

        // Lặp qua 24 giờ (0 -> 23)
        for (let hour = 0; hour < 24; hour++) {
            
            // Tạo GeoJSON FeatureCollection cho giờ này
            const features = apiResponse.map((locationData, index) => {
                // Lấy tọa độ từ VIETNAM_GRID tương ứng
                const coord = VIETNAM_GRID[index];
                // Lấy nhiệt độ tại giờ 'hour'
                const temp = locationData.hourly.temperature_2m[hour];

                return {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [coord.lon, coord.lat]
                    },
                    "properties": {
                        "temperature": temp
                    }
                };
            });

            this.dataCache.push({
                "type": "FeatureCollection",
                "features": features
            });
        }

        // Tải khung hình đầu tiên (0h)
        this.currentHour = 0;
        document.getElementById('hour-slider').value = 0;
        this.drawLayer(this.dataCache[0]);
        
        // Tự động chạy
        this.startAnimation();
    },

    drawLayer: function(geoJson) {
        // Xóa layer cũ
        if (this.map.getLayer('history-heat-layer')) this.map.removeLayer('history-heat-layer');
        if (this.map.getSource('history-heat-source')) this.map.removeSource('history-heat-source');

        // Thêm Source
        this.map.addSource('history-heat-source', {
            type: 'geojson',
            data: geoJson
        });

        // Vẽ Heatmap (Blurred Circles)
        this.map.addLayer({
            id: 'history-heat-layer',
            type: 'circle',
            source: 'history-heat-source',
            paint: {
                // Bán kính thay đổi theo zoom để phủ kín bản đồ
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    5, 50,  
                    10, 100 
                ],
                'circle-blur': 0.8, // Làm nhòe để hòa màu
                'circle-opacity': 0.7,
                
                // Thang màu nhiệt độ
                'circle-color': [
                    'interpolate', ['linear'], ['get', 'temperature'],
                    10, '#3696e1', // Lạnh
                    15, '#a9e3f9',
                    20, '#66ff66', // Mát
                    25, '#ffff66',
                    30, '#ff9933', // Nóng
                    35, '#ff3333'  // Rất nóng
                ]
            }
        }, 'waterway-label');

        // Cập nhật text hiển thị giờ
        const dateStr = document.getElementById('history-date').value;
        document.getElementById('history-time-display').innerHTML = 
            `Thời gian: <strong>${this.currentHour}:00</strong> ngày ${dateStr}`;
    },

    startAnimation: function() {
        this.isPlaying = true;
        document.getElementById('btn-play-history').innerHTML = '<i class="fa-solid fa-pause"></i>';

        if (this.timer) clearInterval(this.timer);
        
        this.timer = setInterval(() => {
            if (!this.isPlaying) return;

            // Tăng giờ
            this.currentHour = (this.currentHour + 1) % 24;
            
            // Cập nhật UI
            document.getElementById('hour-slider').value = this.currentHour;
            
            // Cập nhật dữ liệu bản đồ (Rất nhẹ vì chỉ update source data)
            const source = this.map.getSource('history-heat-source');
            if (source && this.dataCache[this.currentHour]) {
                source.setData(this.dataCache[this.currentHour]);
                
                // Cập nhật text
                const dateStr = document.getElementById('history-date').value;
                document.getElementById('history-time-display').innerHTML = 
                    `Thời gian: <strong>${this.currentHour}:00</strong> ngày ${dateStr}`;
            }

        }, 800); // Tốc độ 800ms / 1 giờ
    },

    stopAnimation: function() {
        this.isPlaying = false;
        document.getElementById('btn-play-history').innerHTML = '<i class="fa-solid fa-play"></i>';
        if (this.timer) clearInterval(this.timer);
    },

    setupControls: function() {
        // Nút Tải dữ liệu
        document.getElementById('btn-load-history').addEventListener('click', () => {
            const dateVal = document.getElementById('history-date').value;
            if (dateVal) this.fetchHistoryData(dateVal);
        });

        // Nút Play/Pause
        document.getElementById('btn-play-history').addEventListener('click', () => {
            if (this.isPlaying) {
                this.stopAnimation();
            } else {
                if (this.dataCache.length > 0) this.startAnimation();
            }
        });

        // Thanh trượt
        document.getElementById('hour-slider').addEventListener('input', (e) => {
            this.stopAnimation();
            this.currentHour = parseInt(e.target.value);
            
            // Update Map ngay lập tức
            const source = this.map.getSource('history-heat-source');
            if (source && this.dataCache[this.currentHour]) {
                source.setData(this.dataCache[this.currentHour]);
                
                const dateStr = document.getElementById('history-date').value;
                document.getElementById('history-time-display').innerHTML = 
                    `Thời gian: <strong>${this.currentHour}:00</strong> ngày ${dateStr}`;
            }
        });
    }
};