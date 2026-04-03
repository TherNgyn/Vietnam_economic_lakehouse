from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.scrapers.interest_rate_crawl_day import scrape_interest_rates_data
from ingestion.ingest_to_bronze import ingest_single_file

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'interest_rate_scraper_dag',
    default_args=default_args,
    description='Scrape interest rates hàng ngày và push vào bronze',
    schedule_interval='0 6 * * *', 
    catchup=False,
)

def scrape_and_append_task():
    """Scrape data và append vào CSV"""
    try:
        
        new_data = scrape_interest_rates_data()
        
        if not new_data:
            return "Không có dữ liệu mới"
        
        csv_path = "historical_dataset/vietnam-interest-rate.csv"
        df_new = pd.DataFrame(new_data)
        if os.path.exists(csv_path):
            df_new.to_csv(csv_path, mode='a', header=False, index=False, encoding="utf-8-sig")
            return f"Appended {len(df_new)} records"
        else:
            df_new.to_csv(csv_path, index=False, encoding="utf-8-sig")
            return f"Created new file with {len(df_new)} records"
            
    except Exception as e:
        raise Exception(f"Scrape lỗi: {str(e)}")

def ingest_task():
    try:
        csv_path = "historical_dataset/vietnam-interest-rate.csv"
        ingest_single_file(csv_path)
        return "Ingest thành công"
    except Exception as e:
        raise Exception(f"Ingest lỗi: {str(e)}")

task_scrape = PythonOperator(
    task_id='scrape_and_append',
    python_callable=scrape_and_append_task,
    dag=dag,
)

task_ingest = PythonOperator(
    task_id='ingest_to_bronze',
    python_callable=ingest_task,
    dag=dag,
)

task_scrape >> task_ingest