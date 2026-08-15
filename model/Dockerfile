FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app/model

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    procps \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY model/requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip \ 
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -c "import pyspark; print('INSTALLED PYSPARK =', pyspark.__version__)"

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONPATH=/app/model

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python


CMD ["tail", "-f", "/dev/null"]