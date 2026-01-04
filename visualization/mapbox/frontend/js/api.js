// export async function fetchWeatherData() {
//     // Lấy dữ liệu 10 ngày từ 2023-12-16 đến 2023-12-25
//     const end = new Date('2023-12-25');
//     const start = new Date(end);
//     start.setDate(start.getDate() - 9);
    
//     const startStr = start.toISOString().split('T')[0];
//     const endStr = end.toISOString().split('T')[0];

//     // const res = await fetch(`http://localhost:3000/api/range?start=${startStr}&end=${endStr}`);
//     const res = await fetch(`/api/range?start=${startStr}&end=${endStr}`);
//     const data = await res.json();
//     return data;
// }

export async function fetchWeatherData() {
    console.log("📡 Đang tải toàn bộ dữ liệu từ Database...");
    
    // Gọi API mà không truyền tham số start/end
    const res = await fetch(`/api/range`);
    
    const data = await res.json();
    
    if (data.length === 0) {
        console.warn("⚠️ Database đang trống!");
    } else {
        console.log(`✅ Đã tải được ${data.length} ngày dữ liệu.`);
    }
    
    return data;
}