const mongoose = require('mongoose');
const axios = require('axios');
const { WeatherModel } = require('./server'); // Import model từ server.js

// Lưới tọa độ (Ví dụ rút gọn, bạn hãy copy VIETNAM_GRID đầy đủ của bạn vào đây)
const GRID = [
    { lat: 21.0285, lon: 105.8542 }, // Hà Nội
    { lat: 10.8231, lon: 106.6297 }, // TP.HCM
    { lat: 16.0544, lon: 108.2022 }  // Đà Nẵng
    // ... thêm các điểm khác
];

async function importData(dateStr) {
    // Kết nối DB (Copy giống bên server.js)
    await mongoose.connect('mongodb://127.0.0.1:27017/weather_db');
    console.log('Bắt đầu tải dữ liệu cho ngày:', dateStr);

    try {
        // Tạo URL request cho toàn bộ Grid
        const lats = GRID.map(p => p.lat).join(',');
        const lons = GRID.map(p => p.lon).join(',');
        const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${lats}&longitude=${lons}&start_date=${dateStr}&end_date=${dateStr}&hourly=temperature_2m,rain,cloudcover,windspeed_10m&timezone=auto`;

        const response = await axios.get(url);
        let results = response.data;
        if (!Array.isArray(results)) results = [results]; // Xử lý trường hợp chỉ có 1 điểm

        // Xóa dữ liệu cũ của ngày này (để tránh trùng lặp)
        await WeatherModel.deleteMany({ date: dateStr });

        // Chuẩn bị dữ liệu để lưu
        const docs = results.map((item, index) => {
            return {
                date: dateStr,
                location: { lat: GRID[index].lat, lon: GRID[index].lon },
                hourly_temps: item.hourly.temperature_2m,
                // --- MAP DỮ LIỆU MỚI ---
                hourly_rain: item.hourly.rain,
                hourly_clouds: item.hourly.cloudcover,
                hourly_wind: item.hourly.windspeed_10m
            };
        });

        // Lưu vào MongoDB
        await WeatherModel.insertMany(docs);
        console.log(`Đã lưu thành công ${docs.length} điểm dữ liệu!`);

    } catch (error) {
        console.error("Lỗi:", error.message);
    } finally {
        mongoose.disconnect();
    }
}

// Chạy hàm nhập liệu cho ngày hôm qua
const yesterday = new Date();
yesterday.setDate(yesterday.getDate() - 1);
const dateString = yesterday.toISOString().split('T')[0];

importData(dateString); 
// Bạn có thể sửa dateString thành '2023-12-01' để tải ngày bất kỳ