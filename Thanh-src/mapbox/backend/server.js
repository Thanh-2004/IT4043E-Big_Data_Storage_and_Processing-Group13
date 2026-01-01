const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

// Import các module
const { GRID } = require('./config'); // Chỉ import để log kiểm tra lúc khởi động
const { syncAndGetWeatherData, clearAllData } = require('./dataLoader');

// --- KIỂM TRA CẤU HÌNH ---
if (!GRID || !Array.isArray(GRID)) {
    console.error("❌ LỖI: Không đọc được GRID từ config.js");
    process.exit(1);
} else {
    console.log(`✅ Cấu hình: ${GRID.length} trạm quan trắc.`);
}

const app = express();
app.use(cors());
app.use(express.json());

// 1. Kết nối MongoDB
mongoose.connect('mongodb://127.0.0.1:27017/weather_db')
    .then(() => console.log('✅ Đã kết nối MongoDB'))
    .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

// 2. API: Lấy dữ liệu (sẽ tự động đồng bộ nếu thiếu)
app.get('/api/range', async (req, res) => {
    try {
        const { start, end } = req.query;
        if (!start || !end) {
            return res.status(400).json({ error: "Thiếu tham số start/end date" });
        }

        // Gọi logic từ module dataLoader
        const data = await syncAndGetWeatherData(start, end);
        
        res.json(data);
    } catch (error) {
        console.error("❌ Lỗi xử lý:", error);
        res.status(500).json({ error: error.message });
    }
});

// 3. API: Xóa dữ liệu
app.delete('/api/clear', async (req, res) => {
    try {
        const result = await clearAllData();
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 4. Khởi động Server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🚀 Server đang chạy tại http://localhost:${PORT}`);
});