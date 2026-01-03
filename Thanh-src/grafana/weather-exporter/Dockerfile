FROM python:3.9-slim

WORKDIR /app

# Cài thư viện cần thiết
RUN pip install pymongo prometheus_client

# Copy code vào
COPY app.py .

# Chạy code
CMD ["python", "app.py"]