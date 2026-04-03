FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install minio pandas selenium webdriver-manager python-dotenv

COPY ingestion/ /app/ingestion/
COPY historical_dataset/ /app/historical_dataset/
COPY .env /app/.env

CMD ["python", "-u", "ingestion/ingest_to_bronze.py"]