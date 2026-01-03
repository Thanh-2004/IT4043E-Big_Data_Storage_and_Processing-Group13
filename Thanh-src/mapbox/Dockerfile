# 1. Chọn môi trường Node.js
FROM node:18-alpine

# 2. Tạo thư mục làm việc trong container
WORKDIR /app

# 3. Copy file quản lý thư viện vào trước (để tận dụng cache)
COPY package*.json ./

# 4. Cài đặt thư viện
RUN npm install --production

# 5. Copy toàn bộ code vào container
COPY . .

# 6. Mở cổng 3000
EXPOSE 3000

# 7. Lệnh chạy server
CMD ["node", "backend/server.js"]git