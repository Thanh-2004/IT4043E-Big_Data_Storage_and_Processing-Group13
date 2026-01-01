const PopupManager = {
    map: null,

    init: function(mapInstance) {
        this.map = mapInstance;
        // Đăng ký sự kiện click
        this.map.on('click', (e) => this.handleClick(e));
        
        // Hiệu ứng con trỏ
        this.map.on('mouseenter', () => this.map.getCanvas().style.cursor = 'crosshair');
        this.map.on('mouseleave', () => this.map.getCanvas().style.cursor = '');
    },

    handleClick: async function(e) {
        const { lng, lat } = e.lngLat;

        // Tạo popup tạm
        const popup = new mapboxgl.Popup({ closeButton: true })
            .setLngLat([lng, lat])
            .setHTML('<div style="color:#aaa">Đang tải dữ liệu...</div>')
            .addTo(this.map);

        try {
            const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lng}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m&timezone=auto`;
            
            const res = await fetch(url);
            const data = await res.json();
            const current = data.current;

            const htmlContent = `
                <h3 style="margin:0 0 10px 0; color: #ff5757;">Thời tiết điểm chọn</h3>
                <div class="weather-stat"><span><i class="fa-solid fa-temperature-half" style="color:orange"></i> Nhiệt độ</span><strong>${current.temperature_2m}°C</strong></div>
                <div class="weather-stat"><span><i class="fa-solid fa-droplet" style="color:cyan"></i> Độ ẩm</span><strong>${current.relative_humidity_2m}%</strong></div>
                <div class="weather-stat"><span><i class="fa-solid fa-wind" style="color:white"></i> Gió</span><strong>${current.wind_speed_10m} km/h</strong></div>
                <div style="margin-top:10px; font-size:10px; color:#888; text-align:right">Lat: ${lat.toFixed(2)}, Lon: ${lng.toFixed(2)}</div>
            `;
            popup.setHTML(htmlContent);
        } catch (err) {
            console.error(err);
            popup.setHTML('<div style="color:red">Lỗi kết nối API!</div>');
        }
    }
};