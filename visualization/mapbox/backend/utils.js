const axios = require('axios');

// Hàm tạo khoảng dừng
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Hàm tính toán danh sách ngày
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

// Hàm chuyển đổi Tốc độ/Hướng -> Vector U/V
function calculateUV(speedArr, dirArr) {
    if (!speedArr || !dirArr || !Array.isArray(speedArr) || !Array.isArray(dirArr)) {
        // Nếu dữ liệu đầu vào bị lỗi/undefined -> Trả về mảng rỗng để không crash server
        return { u: [], v: [] };
    }
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

// Hàm Fetch API có cơ chế Retry (Thử lại khi lỗi 429)
async function fetchWithRetry(url, retries = 5, delayTime = 5000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url);
        } catch (error) {
            if (error.response && error.response.status === 429) {
                console.log(`⚠️ Bị chặn (429). Chờ ${delayTime/1000}s rồi thử lại lần ${i+1}...`);
                await delay(delayTime);
                delayTime += 2000; 
            } else {
                throw error;
            }
        }
    }
    throw new Error('Đã thử lại nhiều lần nhưng vẫn thất bại (429).');
}

module.exports = { delay, getDatesInRange, calculateUV, fetchWithRetry };