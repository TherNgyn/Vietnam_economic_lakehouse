FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install yfinance minio pandas selenium webdriver-manager python-dotenv deltalake pyarrow kafka-python influxdb

COPY .env /app/.env
