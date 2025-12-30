const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
app.use(cors()); // Cho phép Frontend truy cập
app.use(express.json());

// 1. Kết nối MongoDB
mongoose.connect('mongodb://127.0.0.1:27017/weather_db')
    .then(() => console.log('Đã kết nối MongoDB'))
    .catch(err => console.error('Lỗi kết nối MongoDB:', err));

    
// 2. Định nghĩa Schema (Cấu trúc dữ liệu)
const WeatherSchema = new mongoose.Schema({
    date: String, // Format: YYYY-MM-DD
    location: {
        lat: Number,
        lon: Number
    },
    hourly_temps: [Number] // Mảng 24 số thực
});

const WeatherModel = mongoose.model('Weather', WeatherSchema);

// 3. Tạo API lấy dữ liệu
app.get('/api/history', async (req, res) => {
    try {
        const { date } = req.query; // Lấy tham số ?date=...
        
        if (!date) return res.status(400).json({ error: "Thiếu tham số date" });

        // Tìm tất cả bản ghi trùng ngày
        const data = await WeatherModel.find({ date: date });
        
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Khởi chạy server
app.listen(3000, () => {
    console.log('Server đang chạy tại http://localhost:3000');
});

// Export model để dùng ở file nhập liệu
module.exports = { WeatherModel };