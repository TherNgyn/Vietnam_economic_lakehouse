import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def check_kafka_health(**context):
    import requests
    broker_host = os.getenv('KAFKA_BROKER', 'kafka:29092').split(':')[0]
    try:
        # Use kafka-topics via BashOperator output is preferred, but for a quick
        # connectivity check we verify the topic list is returned.
        import socket
        host, port = os.getenv('KAFKA_BROKER', 'kafka:9092').split(':')
        with socket.create_connection((host, int(port)), timeout=10):
            pass
        return f'Kafka broker reachable at {host}:{port}'
    except Exception as e:
        raise Exception(f'Kafka unreachable: {e}')


def check_influxdb_health(**context):
    import requests
    host = os.getenv('INFLUXDB_HOST', 'influxdb')
    port = int(os.getenv('INFLUXDB_PORT', 8086))
    try:
        resp = requests.get(f'http://{host}:{port}/ping', timeout=5)
        if resp.status_code == 204:
            return 'InfluxDB OK'
        raise Exception(f'ping returned {resp.status_code}')
    except Exception as e:
        raise Exception(f'InfluxDB health check failed: {e}')


with DAG(
    dag_id='realtime_streaming_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['realtime', 'kafka', 'streaming', 'influxdb'],
) as dag:

    setup_topics = BashOperator(
        task_id='setup_kafka_topics',
        bash_command=(
            'docker exec kafka kafka-topics '
            '--bootstrap-server kafka:9092 --create --if-not-exists '
            '--topic ticker-realtime --partitions 4 --replication-factor 1 && '
            'docker exec kafka kafka-topics '
            '--bootstrap-server kafka:9092 --create --if-not-exists '
            '--topic index-world-realtime --partitions 4 --replication-factor 1 && '
            'docker exec kafka kafka-topics '
            '--bootstrap-server kafka:9092 --create --if-not-exists '
            '--topic product-realtime --partitions 4 --replication-factor 1 && '
            'docker exec kafka kafka-topics '
            '--bootstrap-server kafka:9092 --create --if-not-exists '
            '--topic index-realtime --partitions 1 --replication-factor 1'
        ),
    )

    kafka_health = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
    )

    influxdb_health = PythonOperator(
        task_id='check_influxdb_health',
        python_callable=check_influxdb_health,
    )

    start_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='docker exec -w /app python_container python /app/kafka/producer_unified.py',
    )

    start_vn_index = BashOperator(
        task_id='start_vn_index_producer',
        bash_command='docker exec -w /app python_container python /app/kafka/producer_index.py',
    )

    start_consumer_bronze = BashOperator(
        task_id='start_consumer_bronze',
        bash_command='docker exec -w /app python_container python /app/kafka/consumer_bronze_group.py',
    )

    start_consumer_influxdb = BashOperator(
        task_id='start_consumer_influxdb',
        bash_command='docker exec -w /app python_container python /app/kafka/consumer_influxdb_group.py',
    )

    setup_topics >> kafka_health >> influxdb_health

