const { GRID } = require('./config');
const { WeatherModel } = require('./models');
const { delay, getDatesInRange, calculateUV, fetchWithRetry } = require('./utils');

console.log("👉 Collection đang dùng là:", WeatherModel.collection.name);

// Hàm chính: Đồng bộ dữ liệu và trả về kết quả
// async function syncAndGetWeatherData(start, end) {
//     const dateList = getDatesInRange(start, end);
//     console.log(`📅 Kiểm tra dữ liệu: ${start} -> ${end}`);

//     // 1. Lấy dữ liệu hiện có trong DB
//     let existingData = await WeatherModel.find({ date: { $in: dateList } });
//     if (!existingData) existingData = [];

//     // 2. Kiểm tra xem ngày nào thiếu trạm (so với GRID hiện tại)
//     const datesToFetch = [];
//     for (const date of dateList) {
//         const recordsForDate = existingData.filter(d => d.date === date);
//         // Nếu số lượng bản ghi ít hơn tổng số trạm trong cấu hình -> Cần tải lại
//         if (recordsForDate.length < GRID.length) {
//             datesToFetch.push(date);
//         }
//     }

//     // 3. Nếu có ngày thiếu, tiến hành tải và cập nhật
//     if (datesToFetch.length > 0) {
//         console.log(`⚠️ Cần cập nhật ${datesToFetch.length} ngày (cho đủ ${GRID.length} trạm)...`);
        
//         const lats = GRID.map(p => p.lat).join(',');
//         const lons = GRID.map(p => p.lon).join(',');

//         for (const date of datesToFetch) {
//             const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${lats}&longitude=${lons}&start_date=${date}&end_date=${date}&hourly=temperature_2m,rain,cloudcover,windspeed_10m,winddirection_10m&timezone=auto`;
            
//             try {
//                 console.log(`⏳ Đang tải ngày: ${date}...`);
                
//                 // Gọi API với cơ chế thử lại (Retry)
//                 const response = await fetchWithRetry(url);
                
//                 let results = response.data;
//                 if (!Array.isArray(results)) results = [results];

//                 // Transform dữ liệu
//                 const docs = results.map((item, index) => {
//                     const speeds = item.hourly.windspeed_10m;
//                     const directions = item.hourly.winddirection_10m;
//                     const { u, v } = calculateUV(speeds, directions); // Tính toán vector

//                     return {
//                         date: date,
//                         location: { lat: GRID[index].lat, lon: GRID[index].lon },
//                         hourly_temps: item.hourly.temperature_2m,
//                         hourly_rain: item.hourly.rain,
//                         hourly_clouds: item.hourly.cloudcover,
//                         hourly_wind: speeds,
//                         hourly_wind_u: u,
//                         hourly_wind_v: v
//                     };
//                 });

//                 // Xóa dữ liệu cũ (thiếu) và lưu mới (đủ)
//                 await WeatherModel.deleteMany({ date: date }); 
//                 await WeatherModel.insertMany(docs);
                
//                 // Nghỉ để tránh 429
//                 await delay(2000); 

//             } catch (err) {
//                 console.error(`❌ Lỗi tải ngày ${date}:`, err.message);
//             }
//         }
        
//         // 4. Lấy lại dữ liệu mới nhất sau khi đã đồng bộ xong
//         existingData = await WeatherModel.find({ date: { $in: dateList } });
//         console.log(`✅ Hoàn tất đồng bộ.`);
//     } else {
//         console.log("⚡️ Dữ liệu đã đầy đủ (Full Cache).");
//     }

//     return existingData;
// }

async function syncAndGetWeatherData(start, end) {
    let query = {}; // Mặc định là query rỗng (nghĩa là lấy tất cả)
    
    // Nếu có truyền start và end thì mới lọc theo ngày
    if (start && end) {
        const dateList = getDatesInRange(start, end);
        query = { date: { $in: dateList } };
        console.log(`🔍 Query DB từ: ${start} đến ${end}`);
    } else {
        console.log(`🔍 Query DB: Lấy TOÀN BỘ dữ liệu đang có.`);
    }

    try {
        // Tìm và sắp xếp theo ngày tăng dần
        const existingData = await WeatherModel.find(query).sort({ date: 1 });

        console.log(`✅ Tìm thấy ${existingData.length} bản ghi.`);
        const mappedData = existingData.map(item => {
            // Chuyển đổi từ Mongoose Document sang Object thường
            const doc = item.toObject ? item.toObject() : item;

            return {
                // Bên trái: Tên Frontend cần --- Bên phải: Tên trong DB của bạn
                date:           doc.timestamp,
                location:       doc.city,
                
                // Mapping dữ liệu mảng
                hourly_temps:   doc.temperature_2m,
                hourly_rain:    doc.precipitation,
                hourly_clouds:  doc.cloud_cover,
                hourly_wind:    doc.wind_speed_10m,

                hourly_wind_u:  doc.wind_direction_10m_y,
                hourly_wind_v:  doc.wind_direction_10m_x
            };
        });

        // 3. Trả về dữ liệu đã được đổi tên chuẩn chỉnh
        return mappedData;

    } catch (err) {
        console.error("❌ Lỗi truy vấn DB:", err);
        throw err;
    }
}

// Hàm xóa toàn bộ dữ liệu
async function clearAllData() {
    await WeatherModel.deleteMany({});
    console.log("🗑️ Đã xóa sạch dữ liệu.");
    return { success: true };
}

module.exports = { syncAndGetWeatherData, clearAllData };