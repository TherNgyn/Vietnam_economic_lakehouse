"""
Interest Rate Scraper from SBV Website
Scrapes missing dates from CSV source, pushes raw data to daily Delta table
- Input: https://www.sbv.gov.vn/vi/lãi-suất-thị-trường-liên-ngân-hàng
- Source CSV: s3://bronze/historical/economics/vietnam-interest-rate.csv (check max date)
- Output: s3://bronze/daily/economics/interest_rate/ (raw, no dedup)
- Strategy: Find latest date in CSV, scrape from that date onward, push raw to daily
"""

import os
import sys
import pandas as pd
import s3fs
from datetime import datetime
from deltalake import write_deltalake
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_S3_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}

def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )

def get_existing_dates():
    """Read existing dates from Bronze historical CSV, return max date"""
    max_date = None
    
    try:
        csv_path = f"s3://{MINIO_BUCKET}/historical/economics/vietnam-interest-rate.csv"
        fs = get_s3fs()
        
        with fs.open(csv_path, 'rb') as f:
            df = pd.read_csv(f, encoding='utf-8')
        
        if len(df) > 0 and 'date' in df.columns:
            # Convert to datetime - dates are in dd/mm/yyyy format
            df['date_obj'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
            max_date = df['date_obj'].max()
            print(f"Read {len(df)} existing records from CSV")
            if pd.notna(max_date):
                print(f"Latest date in CSV: {max_date.strftime('%d/%m/%Y')}")
            else:
                print(f"Latest date in CSV: N/A (date parsing failed)")
    except Exception as e:
        print(f"Note: Bronze historical CSV doesn't exist yet: {e}")
    
    return max_date

def parse_sbv_date(date_text):
    """Parse SBV date format to YYYY-MM-DD, return None if invalid"""
    try:
        date_text = str(date_text).strip()
        
        # Check format: must be dd/mm/yyyy (length 10, with /)
        if not ('/' in date_text and len(date_text) == 10):
            return None
        
        # Try to parse
        date_obj = datetime.strptime(date_text, '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None

def scrape_interest_rates():
    url = "https://www.sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t-th%E1%BB%8B-tr%C6%B0%E1%BB%9Dng-li%C3%AAn-ng%C3%A2n-h%C3%A0ng"
    
    max_date = get_existing_dates()
    
    if pd.notna(max_date):
        # Only scrape dates after max_date in CSV
        from_date = max_date
    else:
        # CSV doesn't exist, scrape from 2014
        from_date = datetime(2014, 2, 1)
    
    from_date_str = from_date.strftime("%d/%m/%Y")
    to_date_str = datetime.now().strftime("%d/%m/%Y")
    
    print(f"Scraping from {from_date_str} to {to_date_str}")
    
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
    
    all_data = []
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 20)
        
        # Fill date inputs
        from_input = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
        to_input = driver.find_element(By.ID, "toDate")
        
        from_input.clear()
        from_input.send_keys(from_date_str)
        time.sleep(1)
        
        to_input.clear()
        to_input.send_keys(to_date_str)
        time.sleep(1)
        
        # Search
        search_btn = driver.find_element(By.ID, "btnSearch")
        driver.execute_script("arguments[0].click();", search_btn)
        time.sleep(5)
        
        page = 1
        while True:
            print(f"Processing page {page}...")
            
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                if not rows:
                    print("No rows found, stopping pagination")
                    break
                
                print(f"Found {len(rows)} rows on page {page}")
                
                for i in range(len(rows)):
                    try:
                        # Re-fetch rows to avoid stale element
                        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                        row = rows[i]
                        cols = row.find_elements(By.TAG_NAME, "td")
                        
                        if len(cols) >= 2:
                            date_text = cols[0].text.strip()
                            date_formatted = parse_sbv_date(date_text)
                            
                            # Skip if date parsing failed (not a valid date)
                            if date_formatted is None:
                                continue
                            
                            # Skip if date already in CSV
                            if pd.notna(max_date):
                                date_obj = pd.to_datetime(date_formatted)
                                if date_obj <= max_date:
                                    continue
                            
                            print(f"Scraping new date: {date_formatted}")
                            
                            # Click to view details
                            links = cols[1].find_elements(By.TAG_NAME, "a")
                            if links:
                                driver.execute_script("arguments[0].click();", links[0])
                                time.sleep(2)
                                
                                # Extract detail rows
                                detail_rows = driver.find_elements(By.CSS_SELECTOR, "#detail-view table tbody tr")
                                for drow in detail_rows:
                                    dcols = drow.find_elements(By.TAG_NAME, "td")
                                    if len(dcols) >= 3:
                                        all_data.append({
                                            'date': date_formatted,
                                            'term': dcols[0].text.strip(),
                                            'interest_rate': dcols[1].text.strip(),
                                            'volume': dcols[2].text.strip(),
                                            'source': 'SBV',
                                            'time_scraped': pd.Timestamp.now()
                                        })
                                
                                time.sleep(1)
                                
                                # Go back
                                try:
                                    back_btn = driver.find_element(By.ID, "btn-back")
                                    driver.execute_script("arguments[0].click();", back_btn)
                                    time.sleep(2)
                                except:
                                    pass
                    except Exception as e:
                        print(f"Error processing row {i}: {e}")
                        continue
                
                # Next page
                try:
                    next_btn = driver.find_element(By.XPATH, "//button[contains(text(),'Sau »')]")
                    if next_btn.get_attribute("disabled") == "disabled":
                        print("No more pages (button disabled)")
                        break
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(3)
                    page += 1
                except Exception as e:
                    print(f"No pagination button found or pagination ended: {e}")
                    break
            
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break
    
    finally:
        driver.quit()
    
    if len(all_data) == 0:
        print("No new data scraped")
        return
    
    print(f"\nTotal new records scraped: {len(all_data)}")
    print(f"New dates: {len(set(d['date'] for d in all_data))}")
    
    # Prepare DataFrame - raw, no deduplication
    df_new = pd.DataFrame(all_data)
    df_new['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    print("\nFirst rows:")
    print(df_new.head(10))
    print("\nLast rows:")
    print(df_new.tail(10))
    
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
        
        print(f"\nSuccessfully scraped and ingested {len(df_new)} records to {delta_path}")
    except Exception as e:
        print(f"Error writing to Delta: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        scrape_interest_rates()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
