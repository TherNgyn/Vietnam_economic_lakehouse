FROM apache/airflow:2.7.0-python3.11

# Cài Chrome/Chromium và các dependencies
RUN apt-get update && apt-get install -y \
    chromium-browser \
    chromium-chromedriver \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libappindicator1 \
    libindicator7 \
    fonts-liberation \
    xdg-utils \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Cài Python dependencies
RUN pip install --no-cache-dir \
    selenium \
    minio \
    pandas \
    webdriver-manager \
    python-dotenv

# Copy requirements.txt nếu có
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt || true