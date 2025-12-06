// Danh sách các điểm lấy mẫu trải dài khắp Việt Nam để tạo Heatmap
const VIETNAM_GRID = [
    // Miền Bắc
    { name: "Ha Giang", lat: 22.8, lon: 104.9 },
    { name: "Cao Bang", lat: 22.6, lon: 106.2 },
    { name: "Lao Cai", lat: 22.4, lon: 103.9 },
    { name: "Dien Bien", lat: 21.3, lon: 103.0 },
    { name: "Son La", lat: 21.3, lon: 104.8 },
    { name: "Lang Son", lat: 21.8, lon: 106.7 },
    { name: "Hanoi", lat: 21.0, lon: 105.8 },
    { name: "Hai Phong", lat: 20.8, lon: 106.6 },
    { name: "Quang Ninh", lat: 21.0, lon: 107.3 },
    { name: "Nam Dinh", lat: 20.4, lon: 106.1 },
    { name: "Thanh Hoa", lat: 19.8, lon: 105.7 },
    
    // Miền Trung
    { name: "Nghe An", lat: 19.0, lon: 105.0 },
    { name: "Ha Tinh", lat: 18.3, lon: 105.9 },
    { name: "Quang Binh", lat: 17.5, lon: 106.6 },
    { name: "Quang Tri", lat: 16.8, lon: 107.1 },
    { name: "Hue", lat: 16.4, lon: 107.6 },
    { name: "Da Nang", lat: 16.0, lon: 108.2 },
    { name: "Quang Nam", lat: 15.6, lon: 107.8 },
    { name: "Kon Tum", lat: 14.3, lon: 108.0 },
    { name: "Gia Lai", lat: 13.9, lon: 108.0 },
    { name: "Binh Dinh", lat: 13.7, lon: 109.2 },
    { name: "Dak Lak", lat: 12.6, lon: 108.0 },
    { name: "Khanh Hoa", lat: 12.2, lon: 109.1 },
    { name: "Lam Dong", lat: 11.9, lon: 108.4 },
    { name: "Ninh Thuan", lat: 11.6, lon: 108.9 },
    { name: "Binh Thuan", lat: 11.1, lon: 108.1 },

    // Miền Nam
    { name: "Tay Ninh", lat: 11.3, lon: 106.1 },
    { name: "Binh Phuoc", lat: 11.7, lon: 106.9 },
    { name: "HCM", lat: 10.8, lon: 106.6 },
    { name: "Vung Tau", lat: 10.5, lon: 107.2 },
    { name: "Dong Thap", lat: 10.5, lon: 105.6 },
    { name: "An Giang", lat: 10.5, lon: 105.1 },
    { name: "Can Tho", lat: 10.0, lon: 105.7 },
    { name: "Kien Giang", lat: 9.9, lon: 105.1 },
    { name: "Soc Trang", lat: 9.6, lon: 105.9 },
    { name: "Ca Mau", lat: 9.1, lon: 105.1 },
    { name: "Phu Quoc", lat: 10.2, lon: 103.9 },
    { name: "Con Dao", lat: 8.6, lon: 106.6 }
];