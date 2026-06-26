FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libxss1 \
    fonts-liberation \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install delta yfinance minio pandas selenium webdriver-manager python-dotenv deltalake pyarrow kafka-python influxdb pyyaml numpy requests fsspec s3fs

COPY .env /app/.env