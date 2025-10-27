FROM python:3.12-alpine3.22

WORKDIR /app

# system deps (for cryptography / ssl etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    musl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# default command does nothing (we'll override in docker-compose run)
CMD ["python", "--version"]
