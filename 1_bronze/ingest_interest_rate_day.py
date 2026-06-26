"""
Interest Rate Scraper for Today - Simple Version
Scrapes today's interest rates and pushes to daily Delta table
"""

import os
import sys
import pandas as pd
from datetime import datetime
from deltalake import write_deltalake
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_S3_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}

def scrape_todays_interest_rates():
    url = "https://www.sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t-th%E1%BB%8B-tr%C6%B0%E1%BB%9Dng-li%C3%AAn-ng%C3%A2n-h%C3%A0ng"
    
    print("Scraping today's interest rates from SBV website...")
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-web-resources')
    options.binary_location = '/usr/bin/chromium'
    
    driver = webdriver.Chrome(
        service=Service('/usr/bin/chromedriver'),
        options=options
    )
    
    all_data = []
    
    try:
        driver.get(url)
        time.sleep(7)  # Wait for page to load
        
        # Get today's date
        today = datetime.now().strftime("%d-%m-%Y")
        
        # Get the table with today's data
        table = driver.find_element(By.ID, "new-information-view")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        print(f"Found {len(rows)} rows on page")
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == 3:
                all_data.append({
                    "date": today,
                    "term": cols[0].text.strip(),
                    "interest_rate": cols[1].text.strip(),
                    "volume": cols[2].text.strip(),
                    "source": "SBV",
                    "time_scraped": pd.Timestamp.now()
                })
        
        print(f"Extracted {len(all_data)} records")
    
    except Exception as e:
        print(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        driver.quit()
    
    if len(all_data) == 0:
        print("No data found to scrape")
        return
    
    print(f"\nTotal records scraped for today: {len(all_data)}")
    
    # Prepare DataFrame
    df_new = pd.DataFrame(all_data)
    df_new['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    print("\nScraped records:")
    print(df_new)
    
    # Write to Delta table (raw, append mode)
    try:
        delta_path = f"s3://{MINIO_BUCKET}/daily/economics/interest_rate"
        
        write_deltalake(
            delta_path,
            df_new,
            mode='append',
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        
        print(f"\nSuccessfully ingested {len(df_new)} records to {delta_path}")
    except Exception as e:
        print(f"Error writing to Delta: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        scrape_todays_interest_rates()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
