"""
Bronze Layer: Gasoline Price Scraper
Scrape from PVOil website and ingest to Bronze
- Input: https://www.pvoil.com.vn/tin-gia-xang-dau
- Output: s3://bronze/raw/product/gasoline/
- Strategy: Ingest daily, check existing dates, stop on existing data
"""

import os
import sys
import pandas as pd
import s3fs
from datetime import datetime
from deltalake import write_deltalake, DeltaTable
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

STORAGE_OPTIONS = {
    "key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
}

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_S3_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}   

URL = "https://www.pvoil.com.vn/tin-gia-xang-dau"

def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )

def get_existing_dates():
    try:
        csv_path = f"s3://{MINIO_BUCKET}/historical/product/gasoline_prices.csv"
        fs = get_s3fs()
        with fs.open(csv_path, 'rb') as f:
            df = pd.read_csv(f)
        if len(df) > 0 and 'date' in df.columns:
            existing_dates = set(df['date'].unique())
            return existing_dates
    except:
        pass
    return set()

def parse_date(date_text):
    try:
        parts = date_text.split('/')
        if len(parts) == 3:
            day, month, year = parts
            return f"{year}-{month:0>2}-{day:0>2}"
    except:
        pass
    return date_text

def scrape_gasoline():
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-dev-tools')
    options.add_argument('--disable-sync')
    options.add_argument('--single-process')
    options.binary_location = '/usr/bin/chromium'
    
    driver = webdriver.Chrome(
        service=Service('/usr/bin/chromedriver'),
        options=options
    )
    
    existing_dates = get_existing_dates()
    print(f"Existing dates in Bronze: {len(existing_dates)}")
    
    data = []
    stopped_early = False
    
    try:
        driver.get(URL)
        wait = WebDriverWait(driver, 20)
        
        select = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlpricedate"))))
        num_options = len(select.options)
        
        print(f"Total dates available: {num_options}")
        
        for i in range(num_options):
            select = Select(driver.find_element(By.ID, "ddlpricedate"))
            date_text = select.options[i].text.strip()
            date_formatted = parse_date(date_text)
            
            if date_formatted in existing_dates:
                print(f"Date {date_formatted} already exists, stopping scrape")
                stopped_early = True
                break
            
            select.select_by_index(i)
            time.sleep(2)
            
            try:
                table = driver.find_element(By.CSS_SELECTOR, "div.oilpricescontainer table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                for row in rows[1:]:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) == 4:
                        product = cols[1].text.strip()
                        price_text = cols[2].text.strip().replace(" đ", "").replace(".", "").replace(",", "")
                        change = cols[3].text.strip()
                        
                        data.append({
                            'date': date_formatted,
                            'product': product,
                            'price': float(price_text) if price_text else None,
                            'change': change,
                            'unit': 'VND/liter',
                            'source': URL,
                            'time_scraped': pd.Timestamp.now()
                        })
                
                print(f"Scraped {date_formatted}: {len(rows)-1} products")
            except Exception as e:
                print(f"Error scraping {date_formatted}: {e}")
                continue
        
    finally:
        driver.quit()
    
    if len(data) == 0:
        print("No new data scraped")
        return
    
    df = pd.DataFrame(data)
    df['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    print(f"Total records to ingest: {len(df)}")
    print(df.head())
    
    try:
        delta_path = f"s3://{MINIO_BUCKET}/daily/product/gasoline"
        
        mode = 'append' if not stopped_early else 'append'
        
        write_deltalake(
            delta_path,
            df,
            mode=mode,
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        
        print(f"Successfully ingested {len(df)} records to {delta_path}")
    except Exception as e:
        print(f"Error writing to Delta: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        scrape_gasoline()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
