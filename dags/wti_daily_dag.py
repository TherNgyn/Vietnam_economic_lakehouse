"""
WTI Crude Oil Futures - Daily Real-time Data Ingestion DAG
Scrapes real-time WTI (May 2026) futures data and pushes to bronze/silver layers
Schedule: Daily at market hours
"""

from datetime import datetime, timedelta
import json
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Add ingestion folder to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'silver'))

default_args = {
    'owner': 'vietnam-economic-lakehouse',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email_on_failure': False,
}

dag = DAG(
    'wti_daily_pipeline',
    default_args=default_args,
    description='Daily real-time WTI crude oil futures data ingestion',
    schedule_interval='0 9,14,17 * * 1-5',  # 9am, 2pm, 5pm on weekdays (market hours)
    catchup=False,
    tags=['commodity', 'wti', 'real-time', 'daily'],
)


def scrape_wti_task(**context):
    """Task 1: Scrape real-time WTI data"""
    from ingestion.scrapers.wti_scraper import get_wti_data
    
    print("=" * 50)
    print("TASK 1: Scraping real-time WTI data...")
    print("=" * 50)
    
    data = get_wti_data(source="investing")
    
    if not data:
        raise Exception("Failed to scrape WTI data")
    
    # Save to XCom for next task
    context['task_instance'].xcom_push(key='wti_data', value=data)
    
    print(f"✓ Scraped data:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    return data


def ingest_to_bronze_task(**context):
    """Task 2: Store raw data to bronze layer (MinIO)"""
    
    # Get data from previous task
    wti_data = context['task_instance'].xcom_pull(task_ids='scrape_wti', key='wti_data')
    
    print("=" * 50)
    print("TASK 2: Ingesting to Bronze Layer...")
    print("=" * 50)
    
    import pandas as pd
    from datetime import datetime
    
    # Create DataFrame
    df = pd.DataFrame([wti_data])
    
    # Bronze path: s3://bronze/commodity/wti/YYYY-MM-DD/
    timestamp = datetime.fromisoformat(wti_data['timestamp'])
    date_partition = timestamp.strftime('%Y-%m-%d')
    hour_partition = timestamp.strftime('%H')
    
    bronze_path = f"s3://bronze/commodity/wti/{date_partition}/"
    file_name = f"wti_{hour_partition}_{timestamp.strftime('%H%M%S')}.parquet"
    
    print(f"  Writing to: {bronze_path}{file_name}")
    print(f"  Records: {len(df)}")
    
    # TODO: Implement actual S3/MinIO write
    # df.to_parquet(f"{bronze_path}{file_name}")
    
    # For now, save locally for testing
    local_bronze = f"./data/bronze/commodity/wti/{date_partition}/"
    os.makedirs(local_bronze, exist_ok=True)
    df.to_parquet(f"{local_bronze}{file_name}")
    
    context['task_instance'].xcom_push(key='bronze_path', value=f"{local_bronze}{file_name}")
    print(f"✓ Stored to bronze layer: {local_bronze}{file_name}")
    
    return f"{local_bronze}{file_name}"


def transform_to_silver_task(**context):
    """Task 3: Clean and transform data to silver layer"""
    
    # Get data from bronze task
    bronze_path = context['task_instance'].xcom_pull(task_ids='ingest_to_bronze', key='bronze_path')
    wti_data = context['task_instance'].xcom_pull(task_ids='scrape_wti', key='wti_data')
    
    print("=" * 50)
    print("TASK 3: Transforming to Silver Layer...")
    print("=" * 50)
    
    import pandas as pd
    from silver.wti_clean import clean_wti_data, validate_wti_schema
    
    # Read from bronze
    df = pd.DataFrame([wti_data])
    
    # Clean and validate
    df_clean = clean_wti_data(df)
    is_valid = validate_wti_schema(df_clean)
    
    if not is_valid:
        raise Exception("Data validation failed")
    
    # Silver path: s3://silver/commodity/wti/YYYY-MM-DD/
    timestamp = datetime.fromisoformat(wti_data['timestamp'])
    date_partition = timestamp.strftime('%Y-%m-%d')
    
    silver_path = f"./data/silver/commodity/wti/{date_partition}/"
    os.makedirs(silver_path, exist_ok=True)
    
    # Save as Parquet with schema enforcement
    silver_file = f"{silver_path}wti_cleaned_{timestamp.strftime('%H%M%S')}.parquet"
    df_clean.to_parquet(silver_file, index=False)
    
    print(f"✓ Transformed and stored to silver layer: {silver_file}")
    print(f"  Records: {len(df_clean)}")
    print(f"  Columns: {', '.join(df_clean.columns)}")
    
    context['task_instance'].xcom_push(key='silver_path', value=silver_file)
    return silver_file


def validate_and_notify_task(**context):
    """Task 4: Final validation and notification"""
    
    wti_data = context['task_instance'].xcom_pull(task_ids='scrape_wti', key='wti_data')
    silver_path = context['task_instance'].xcom_pull(task_ids='transform_to_silver', key='silver_path')
    
    print("=" * 50)
    print("TASK 4: Final Validation & Summary")
    print("=" * 50)
    
    print(f"\n✓ Pipeline completed successfully!")
    print(f"  Timestamp: {wti_data['timestamp']}")
    print(f"  Contract: {wti_data['contract']}")
    print(f"  Price: ${wti_data['price']:.2f}")
    print(f"  Change: {wti_data['change']:+.2f} ({wti_data['change_percent']:+.2f}%)")
    print(f"  Silver output: {silver_path}")
    
    return {
        'status': 'success',
        'timestamp': wti_data['timestamp'],
        'price': wti_data['price'],
        'silver_path': silver_path
    }


# Define tasks
task_scrape = PythonOperator(
    task_id='scrape_wti',
    python_callable=scrape_wti_task,
    dag=dag,
)

task_bronze = PythonOperator(
    task_id='ingest_to_bronze',
    python_callable=ingest_to_bronze_task,
    dag=dag,
)

task_silver = PythonOperator(
    task_id='transform_to_silver',
    python_callable=transform_to_silver_task,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_and_notify',
    python_callable=validate_and_notify_task,
    dag=dag,
)

# Task dependencies
task_scrape >> task_bronze >> task_silver >> task_validate
