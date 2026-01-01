const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

// 1. Kết nối MongoDB
mongoose.connect('mongodb://127.0.0.1:27017/weather_db')
    .then(() => console.log('✅ Đã kết nối MongoDB'))
    .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

// 2. Schema
const WeatherSchema = new mongoose.Schema({
    date: String,
    location: { lat: Number, lon: Number },
    hourly_temps: [Number],
    hourly_rain: [Number],
    hourly_clouds: [Number],
    hourly_wind: [Number],
    hourly_wind_u: [Number],
    hourly_wind_v: [Number]
});
const WeatherModel = mongoose.model('Weather', WeatherSchema);

// 3. GRID TỌA ĐỘ: 63 ĐIỂM (TOÀN BỘ TỈNH THÀNH + ĐẢO)
const GRID = [
    // ========== VÙNG 1: MIỀN BẮC (40 điểm) ==========
    
    // --- Hàng 1: Biên giới phía Bắc ---
    { lat: 23.00, lon: 102.80 }, // Lai Châu (tây bắc)
    { lat: 23.00, lon: 103.50 }, // Lai Châu - Lào Cai
    { lat: 23.00, lon: 104.20 }, // Lào Cai
    { lat: 22.90, lon: 104.90 }, // Hà Giang (tây)
    { lat: 22.90, lon: 105.60 }, // Hà Giang (trung tâm)
    { lat: 22.80, lon: 106.30 }, // Cao Bằng
    { lat: 22.70, lon: 107.00 }, // Lạng Sơn (bắc)
    
    // --- Hàng 2 ---
    { lat: 22.30, lon: 103.00 }, // Điện Biên (bắc)
    { lat: 22.30, lon: 103.70 }, // Điện Biên - Lào Cai
    { lat: 22.30, lon: 104.40 }, // Yên Bái (bắc)
    { lat: 22.20, lon: 105.10 }, // Tuyên Quang (tây)
    { lat: 22.10, lon: 105.80 }, // Tuyên Quang
    { lat: 22.00, lon: 106.50 }, // Bắc Kạn
    { lat: 21.90, lon: 107.20 }, // Lạng Sơn (nam)
    
    // --- Hàng 3 ---
    { lat: 21.60, lon: 103.20 }, // Sơn La (bắc)
    { lat: 21.60, lon: 103.90 }, // Sơn La (đông)
    { lat: 21.60, lon: 104.60 }, // Yên Bái
    { lat: 21.50, lon: 105.30 }, // Phú Thọ (tây)
    { lat: 21.50, lon: 106.00 }, // Thái Nguyên
    { lat: 21.40, lon: 106.70 }, // Bắc Giang
    { lat: 21.30, lon: 107.40 }, // Quảng Ninh (tây)
    
    // --- Hàng 4 ---
    { lat: 20.90, lon: 103.50 }, // Sơn La (nam)
    { lat: 20.90, lon: 104.20 }, // Hòa Bình (tây)
    { lat: 20.90, lon: 104.90 }, // Hòa Bình
    { lat: 20.80, lon: 105.60 }, // Vĩnh Phúc / Hà Nội (tây)
    { lat: 20.80, lon: 106.30 }, // Bắc Ninh / Hải Dương
    { lat: 20.70, lon: 107.00 }, // Hải Phòng / Quảng Ninh
    
    // --- Hàng 5: Vùng Hà Nội và ĐBSH ---
    { lat: 20.20, lon: 105.00 }, // Ninh Bình (tây)
    { lat: 20.10, lon: 105.70 }, // Hà Nam / Nam Định (tây)
    { lat: 20.00, lon: 106.40 }, // Nam Định / Thái Bình
    { lat: 19.90, lon: 107.10 }, // Khu vực ven biển
    
    // --- Hàng 6: Nam đồng bằng Bắc Bộ ---
    { lat: 19.50, lon: 105.20 }, // Thanh Hóa (bắc tây)
    { lat: 19.40, lon: 105.90 }, // Thanh Hóa (bắc)
    { lat: 19.30, lon: 106.60 }, // Thanh Hóa (ven biển)
    
    // ========== VÙNG 2: BẮC TRUNG BỘ (25 điểm) ==========
    
    // --- Hàng 7 ---
    { lat: 18.80, lon: 104.80 }, // Thanh Hóa (tây nam)
    { lat: 18.70, lon: 105.50 }, // Nghệ An (tây)
    { lat: 18.60, lon: 106.20 }, // Nghệ An (Vinh)
    { lat: 18.50, lon: 106.90 }, // Nghệ An (ven biển)
    
    // --- Hàng 8 ---
    { lat: 18.10, lon: 105.10 }, // Nghệ An (tây nam)
    { lat: 18.00, lon: 105.80 }, // Hà Tĩnh (tây)
    { lat: 17.90, lon: 106.50 }, // Hà Tĩnh
    { lat: 17.80, lon: 107.20 }, // Hà Tĩnh (ven biển)
    
    // --- Hàng 9 ---
    { lat: 17.40, lon: 105.50 }, // Quảng Bình (tây)
    { lat: 17.30, lon: 106.20 }, // Quảng Bình
    { lat: 17.20, lon: 106.90 }, // Quảng Bình (đông)
    { lat: 17.10, lon: 107.60 }, // Quảng Bình (ven biển)
    
    // --- Hàng 10 ---
    { lat: 16.70, lon: 106.30 }, // Quảng Trị (tây)
    { lat: 16.60, lon: 107.00 }, // Quảng Trị
    { lat: 16.50, lon: 107.70 }, // Quảng Trị (ven biển)
    
    // --- Hàng 11: Huế và miền trung ---
    { lat: 16.10, lon: 107.00 }, // Thừa Thiên Huế (tây)
    { lat: 16.00, lon: 107.70 }, // Huế
    { lat: 15.90, lon: 108.40 }, // Huế (ven biển)
    
    // --- Hàng 12 ---
    { lat: 15.50, lon: 107.40 }, // Quảng Nam (tây)
    { lat: 15.40, lon: 108.10 }, // Đà Nẵng / Quảng Nam
    { lat: 15.30, lon: 108.80 }, // Quảng Nam (ven biển)
    
    // --- Hàng 13 ---
    { lat: 14.90, lon: 108.00 }, // Quảng Nam (nam)
    { lat: 14.80, lon: 108.70 }, // Quảng Ngãi (bắc)
    { lat: 14.70, lon: 109.40 }, // Quảng Ngãi (ven biển)
    
    // ========== VÙNG 3: TÂY NGUYÊN (18 điểm) ==========
    
    // --- Cột Tây (Biên giới) ---
    { lat: 14.80, lon: 107.30 }, // Kon Tum (tây bắc)
    { lat: 14.10, lon: 107.30 }, // Gia Lai (tây bắc)
    { lat: 13.40, lon: 107.40 }, // Gia Lai (tây)
    { lat: 12.70, lon: 107.50 }, // Đắk Lắk (tây)
    { lat: 12.00, lon: 107.60 }, // Lâm Đồng (tây bắc)
    { lat: 11.30, lon: 107.70 }, // Lâm Đồng (tây nam)
    
    // --- Cột Trung tâm ---
    { lat: 14.50, lon: 108.00 }, // Kon Tum
    { lat: 13.80, lon: 108.10 }, // Gia Lai (Pleiku)
    { lat: 13.10, lon: 108.20 }, // Đắk Lắk (Buôn Ma Thuột)
    { lat: 12.40, lon: 108.30 }, // Đắk Nông
    { lat: 11.70, lon: 108.40 }, // Lâm Đồng (Đà Lạt)
    { lat: 11.00, lon: 108.50 }, // Lâm Đồng (nam)
    
    // --- Cột Đông (Tiếp giáp ven biển) ---
    { lat: 14.20, lon: 108.70 }, // Gia Lai (đông)
    { lat: 13.50, lon: 108.80 }, // Đắk Lắk (đông)
    { lat: 12.80, lon: 108.90 }, // Đắk Nông (đông)
    { lat: 12.10, lon: 109.00 }, // Lâm Đồng (đông)
    { lat: 11.40, lon: 109.10 }, // Ninh Thuận / Bình Thuận (tây)
    { lat: 10.70, lon: 108.20 }, // Bình Thuận (tây nam)
    
    // ========== VÙNG 4: NAM TRUNG BỘ (12 điểm) ==========
    
    { lat: 14.10, lon: 109.10 }, // Bình Định (tây)
    { lat: 14.00, lon: 109.80 }, // Bình Định (Quy Nhơn)
    
    { lat: 13.40, lon: 109.20 }, // Phú Yên (tây)
    { lat: 13.30, lon: 109.90 }, // Phú Yên (Tuy Hòa)
    
    { lat: 12.70, lon: 109.30 }, // Khánh Hòa (tây)
    { lat: 12.50, lon: 109.90 }, // Khánh Hòa (Nha Trang)
    
    { lat: 12.00, lon: 109.20 }, // Ninh Thuận (trung tâm)
    { lat: 11.80, lon: 109.80 }, // Ninh Thuận (Phan Rang)
    
    { lat: 11.20, lon: 108.70 }, // Bình Thuận (tây)
    { lat: 11.00, lon: 109.30 }, // Bình Thuận (Phan Thiết)
    { lat: 10.80, lon: 109.90 }, // Bình Thuận (ven biển đông)
    
    { lat: 10.20, lon: 108.80 }, // Bình Thuận (cực nam)
    
    // ========== VÙNG 5: ĐÔNG NAM BỘ (15 điểm) ==========
    
    // --- Vùng Bắc (Bình Phước, Tây Ninh) ---
    { lat: 11.70, lon: 106.50 }, // Bình Phước (bắc)
    { lat: 11.50, lon: 107.20 }, // Bình Phước (đông bắc)
    { lat: 11.20, lon: 106.00 }, // Tây Ninh (tây)
    { lat: 11.00, lon: 106.70 }, // Tây Ninh / Bình Dương
    
    // --- Vùng Trung tâm (TP.HCM, Bình Dương, Đồng Nai) ---
    { lat: 11.00, lon: 107.40 }, // Đồng Nai (bắc)
    { lat: 10.70, lon: 106.30 }, // Bình Dương (nam)
    { lat: 10.50, lon: 107.00 }, // TP.HCM / Đồng Nai
    { lat: 10.30, lon: 107.70 }, // Đồng Nai (đông)
    
    // --- Vùng Nam (Bà Rịa - Vũng Tàu) ---
    { lat: 10.60, lon: 107.40 }, // Đồng Nai (nam đông)
    { lat: 10.30, lon: 107.10 }, // Bà Rịa - Vũng Tàu (tây)
    { lat: 10.10, lon: 107.60 }, // Vũng Tàu
    
    // --- Điểm bổ sung khu vực TP.HCM ---
    { lat: 10.80, lon: 106.70 }, // TP.HCM (bắc)
    { lat: 10.50, lon: 106.50 }, // TP.HCM (tây)
    { lat: 10.30, lon: 106.70 }, // TP.HCM (trung tâm)
    { lat: 10.10, lon: 106.40 }, // TP.HCM (nam)
    
    // ========== VÙNG 6: ĐỒNG BẰNG SÔNG CỬU LONG (30 điểm) ==========
    
    // --- Hàng 1: Vùng Bắc ĐBSCL ---
    { lat: 10.60, lon: 105.50 }, // Long An (bắc tây)
    { lat: 10.50, lon: 106.10 }, // Long An
    { lat: 10.40, lon: 106.70 }, // Tiền Giang (tây)
    
    // --- Hàng 2 ---
    { lat: 10.20, lon: 105.20 }, // An Giang (bắc)
    { lat: 10.10, lon: 105.80 }, // Đồng Tháp (bắc)
    { lat: 10.00, lon: 106.40 }, // Tiền Giang / Vĩnh Long
    { lat: 9.90, lon: 107.00 }, // Bến Tre (bắc)
    
    // --- Hàng 3 ---
    { lat: 9.80, lon: 104.90 }, // An Giang (Long Xuyên)
    { lat: 9.70, lon: 105.50 }, // Đồng Tháp (Cao Lãnh)
    { lat: 9.60, lon: 106.10 }, // Vĩnh Long
    { lat: 9.50, lon: 106.70 }, // Bến Tre (trung tâm)
    
    // --- Hàng 4 ---
    { lat: 9.40, lon: 105.10 }, // Kiên Giang (đông bắc)
    { lat: 9.30, lon: 105.70 }, // Hậu Giang (bắc)
    { lat: 9.20, lon: 106.30 }, // Trà Vinh (bắc)
    
    // --- Hàng 5: Trung tâm ĐBSCL ---
    { lat: 9.10, lon: 104.80 }, // Kiên Giang (Rạch Giá - bắc)
    { lat: 9.00, lon: 105.40 }, // Cần Thơ
    { lat: 8.90, lon: 106.00 }, // Hậu Giang
    { lat: 8.80, lon: 106.60 }, // Trà Vinh (nam)
    
    // --- Hàng 6 ---
    { lat: 8.70, lon: 105.10 }, // Kiên Giang (Rạch Giá)
    { lat: 8.60, lon: 105.70 }, // Sóc Trăng (tây)
    { lat: 8.50, lon: 106.30 }, // Sóc Trăng
    
    // --- Hàng 7: Vùng Nam ĐBSCL ---
    { lat: 8.30, lon: 104.80 }, // Kiên Giang (nam)
    { lat: 8.20, lon: 105.40 }, // Bạc Liêu (bắc)
    { lat: 8.10, lon: 106.00 }, // Bạc Liêu
    { lat: 8.00, lon: 106.60 }, // Sóc Trăng (cực nam)
    
    // --- Hàng 8: Cực Nam ---
    { lat: 9.00, lon: 104.50 }, // Kiên Giang (ven biển tây)
    { lat: 8.70, lon: 104.50 }, // Kiên Giang (Phú Quốc khu vực)
    { lat: 8.50, lon: 104.90 }, // Cà Mau (tây bắc)
    { lat: 8.80, lon: 105.00 }, // Cà Mau (bắc)
    { lat: 8.60, lon: 105.20 }, // Cà Mau (trung tâm)
    { lat: 8.50, lon: 105.50 }, // Cà Mau (đông)
];

// ... (Giữ nguyên các hàm helper và logic Smart Sync cũ)
// Hàm hỗ trợ
function getDatesInRange(startDate, endDate) {
    const date = new Date(startDate);
    const end = new Date(endDate);
    const dates = [];
    while (date <= end) {
        dates.push(date.toISOString().split('T')[0]);
        date.setDate(date.getDate() + 1);
    }
    return dates;
}

function calculateUV(speedArr, dirArr) {
    const u = [];
    const v = [];
    for (let i = 0; i < speedArr.length; i++) {
        const speed = speedArr[i];
        const dir = dirArr[i];
        const rad = dir * (Math.PI / 180);
        u.push(speed * -Math.sin(rad));
        v.push(speed * -Math.cos(rad));
    }
    return { u, v };
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// --- HÀM MỚI: GỌI API CÓ THỬ LẠI (RETRY) ---
async function fetchWithRetry(url, retries = 5, delayTime = 5000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url);
        } catch (error) {
            if (error.response && error.response.status === 429) {
                console.log(`⚠️ Bị chặn (429). Chờ ${delayTime/1000}s rồi thử lại lần ${i+1}...`);
                await delay(delayTime);
                // Tăng thời gian chờ cho lần sau nếu vẫn lỗi
                delayTime += 2000; 
            } else {
                throw error; // Lỗi khác thì ném ra luôn
            }
        }
    }
    throw new Error('Đã thử lại nhiều lần nhưng vẫn thất bại (429).');
}

// API Range
app.get('/api/range', async (req, res) => {
    try {
        const { start, end } = req.query;
        if (!start || !end) return res.status(400).json({ error: "Thiếu start/end date" });

        const dateList = getDatesInRange(start, end);
        console.log(`📅 Kiểm tra: ${start} -> ${end}`);

        let existingData = await WeatherModel.find({ date: { $in: dateList } });
        const datesToFetch = [];

        for (const date of dateList) {
            const recordsForDate = existingData.filter(d => d.date === date);
            if (recordsForDate.length < GRID.length) {
                datesToFetch.push(date);
            }
        }

        if (datesToFetch.length > 0) {
            console.log(`⚠️ Cập nhật ${datesToFetch.length} ngày (cho đủ ${GRID.length} trạm)...`);
            const lats = GRID.map(p => p.lat).join(',');
            const lons = GRID.map(p => p.lon).join(',');

            for (const date of datesToFetch) {
                const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${lats}&longitude=${lons}&start_date=${date}&end_date=${date}&hourly=temperature_2m,rain,cloudcover,windspeed_10m,winddirection_10m&timezone=auto`;
                try {
                    console.log(`⏳ Đang tải lại ngày: ${date}...`);
                    const response = await fetchWithRetry(url);
                    let results = response.data;
                    if (!Array.isArray(results)) results = [results];

                    const docs = results.map((item, index) => {
                        const speeds = item.hourly.windspeed_10m;
                        const directions = item.hourly.winddirection_10m;
                        const { u, v } = calculateUV(speeds, directions);
                        return {
                            date: date,
                            location: { lat: GRID[index].lat, lon: GRID[index].lon },
                            hourly_temps: item.hourly.temperature_2m,
                            hourly_rain: item.hourly.rain,
                            hourly_clouds: item.hourly.cloudcover,
                            hourly_wind: speeds,
                            hourly_wind_u: u,
                            hourly_wind_v: v
                        };
                    });

                    await WeatherModel.deleteMany({ date: date }); 
                    await WeatherModel.insertMany(docs);
                    await delay(300); 

                } catch (err) {
                    console.error(`❌ Lỗi tải ngày ${date}:`, err.message);
                }
            }
            existingData = await WeatherModel.find({ date: { $in: dateList } });
            console.log(`✅ Hoàn tất đồng bộ.`);
        }
        res.json(existingData);

    } catch (error) {
        console.error("❌ Lỗi Server:", error);
        res.status(500).json({ error: error.message });
    }
});

// API Xóa DB
app.delete('/api/clear', async (req, res) => {
    try {
        await WeatherModel.deleteMany({});
        res.json({ success: true });
    } catch (e) { res.status(500).json(e); }
});

app.listen(3000, () => {
    console.log('🚀 Server chạy tại http://localhost:3000');
});