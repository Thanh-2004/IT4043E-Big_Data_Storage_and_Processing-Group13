export const Config = {
    // Cấu hình Mapbox
    mapboxToken: window.env?.MAPBOX_TOKEN,
    
    // Cấu hình Gió
    haloRadius: 0.8,
    maxLinesPerStation: 50, 
    lineSpeed: 0.025,       // Tốc độ di chuyển
    maxRadiusKm: 35,        // Giới hạn phạm vi bay
    spawnRadius: 0.4,       // Phạm vi sinh ra
    targetVisualLengthKm: 8, // Độ dài hiển thị mục tiêu

    // --- CẤU HÌNH MÀU SẮC ---
    colors: {
        // Gió: Xanh dương -> Đỏ (theo tốc độ km/h)
        wind: [
            'interpolate', ['linear'], ['get', 'speed'],
            0, '#2196f3', 15, '#00e676', 30, '#ffeb3b', 45, '#ff9800', 60, '#f44336', 100, '#d50000'
        ],
        // Nhiệt độ: Tím (Lạnh) -> Vàng -> Đỏ (Nóng) (theo độ C)
        temp: [
            'interpolate', ['linear'], ['get', 'temp'],
            0, '#4a148c',    // Rất lạnh
            10, '#2196f3',   // Lạnh
            20, '#00e676',   // Mát
            28, '#ffeb3b',   // Ấm
            35, '#ff5722',   // Nóng
            40, '#d50000'    // Rất nóng
        ],
        // Mưa: Trong suốt -> Xanh đậm (theo mm)
        rain: [
            'interpolate', ['linear'], ['get', 'rain'],
            0, 'rgba(0,0,0,0)', // Không mưa (trong suốt)
            0.1, '#81d4fa',     // Mưa nhỏ
            5, '#0288d1',       // Mưa vừa
            20, '#01579b'       // Mưa to
        ],
        // Mây: Trong suốt -> Xám trắng (theo %)
        cloud: [
            'interpolate', ['linear'], ['get', 'cloud'],
            0, 'rgba(0,0,0,0)', // trời quang
            20, 'rgba(255,255,255,0.2)', 
            50, 'rgba(255,255,255,0.5)',
            100, 'rgba(200,200,200,0.9)' // Nhiều mây
        ]
    },
    legends: {
        wind: {
            title: "Tốc độ gió (km/h)",
            background: "linear-gradient(to right, #2196f3, #00e676, #ffeb3b, #f44336, #d50000)",
            min: "0", max: "100+"
        },
        temp: {
            title: "Nhiệt độ (°C)",
            background: "linear-gradient(to right, #4a148c, #2196f3, #00e676, #ffeb3b, #ff5722, #d50000)",
            min: "0°", max: "40°+"
        },
        rain: {
            title: "Lượng mưa (mm)",
            background: "linear-gradient(to right, rgba(0,0,0,0), #81d4fa, #0288d1, #01579b)",
            min: "0", max: "20+"
        },
        cloud: {
            title: "Độ phủ mây (%)",
            background: "linear-gradient(to right, #333, #666, #999, #ddd)", // Mô phỏng màu mây trên nền tối
            min: "0%", max: "100%"
        }
    }
};