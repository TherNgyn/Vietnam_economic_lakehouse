FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libxss1 \
    fonts-liberation \
    xdg-utils \
    libreoffice \
    && rm -rf /var/lib/apt/lists/* 

RUN pip install --upgrade pip && \
    pip install \
    pandas \
    pyarrow \
    numpy==1.26.4 \
    minio \
    requests \
    beautifulsoup4 \
    delta \
    yfinance \
    selenium \
    webdriver-manager \
    python-dotenv \
    deltalake \
    kafka-python \
    influxdb \
    pyyaml \
    fsspec \
    s3fs \
    pyspark \
    boto3 \
    dbt-spark[PyHive]
    
