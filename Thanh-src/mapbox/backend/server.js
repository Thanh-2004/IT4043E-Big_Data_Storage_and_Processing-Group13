const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

// Import các module đã tách
const { GRID } = require('./config');
const { WeatherModel } = require('./models');
const { delay, getDatesInRange, calculateUV, fetchWithRetry } = require('./utils');

const app = express();
app.use(cors());
app.use(express.json());

// Kết nối MongoDB
mongoose.connect('mongodb://127.0.0.1:27017/weather_db')
    .then(() => console.log('✅ Đã kết nối MongoDB'))
    .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

// API Range
app.get('/api/range', async (req, res) => {
    try {
        const { start, end } = req.query;
        if (!start || !end) return res.status(400).json({ error: "Thiếu start/end date" });

        const dateList = getDatesInRange(start, end);
        console.log(`📅 Kiểm tra: ${start} -> ${end}`);

        let existingData = await WeatherModel.find({ date: { $in: dateList } });
        if (!existingData) existingData = [];
        const datesToFetch = [];

        // Logic kiểm tra thiếu trạm
        for (const date of dateList) {
            const recordsForDate = existingData.filter(d => d.date === date);
            if (recordsForDate.length < GRID.length) {
                datesToFetch.push(date);
            }
        }

        if (datesToFetch.length > 0) {
            console.log(`⚠️ Cần cập nhật ${datesToFetch.length} ngày (cho đủ ${GRID.length} trạm)...`);
            const lats = GRID.map(p => p.lat).join(',');
            const lons = GRID.map(p => p.lon).join(',');

            for (const date of datesToFetch) {
                const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${lats}&longitude=${lons}&start_date=${date}&end_date=${date}&hourly=temperature_2m,rain,cloudcover,windspeed_10m,winddirection_10m&timezone=auto`;
                
                try {
                    console.log(`⏳ Đang tải ngày: ${date}...`);
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
                    await delay(2000); // Nghỉ 2s

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

// API Clear
app.delete('/api/clear', async (req, res) => {
    try {
        await WeatherModel.deleteMany({});
        res.json({ success: true });
    } catch (e) { res.status(500).json(e); }
});

app.listen(3000, () => {
    console.log('🚀 Server chạy tại http://localhost:3000');
});