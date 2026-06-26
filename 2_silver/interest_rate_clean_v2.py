"""
Silver Layer: Interest Rate Cleaning
Read from Bronze (historical CSV + daily scraped data), clean, and append to Silver
- Input: s3://bronze/historical/economics/vietnam-interest-rate.csv (historical)
         s3://bronze/daily/economics/interest_rate/ (daily scraped)
- Output: s3://bronze/indicators/economy/interest_rate/processing_date={date}/
"""

import os
import sys
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime
from deltalake import write_deltalake

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

TERM_MAPPING = {
    'Qua đêm': ('Overnight', 'ON'),
    '1 Tuần': ('1 Week', '1W'),
    '2 Tuần': ('2 Weeks', '2W'),
    '1 Tháng': ('1 Month', '1M'),
    '2 Tháng': ('2 Months', '2M'),
    '3 Tháng': ('3 Months', '3M'),
    '6 Tháng': ('6 Months', '6M'),
    '9 Tháng': ('9 Months', '9M'),
    '12 Tháng': ('12 Months', '12M'),
}

def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )

def read_historical_csv():
    try:
        csv_path = f"s3://{MINIO_BUCKET}/historical/economics/vietnam-interest-rate.csv"
        fs = get_s3fs()
        with fs.open(csv_path, 'rb') as f:
            df = pd.read_csv(f, encoding='utf-8', on_bad_lines='skip')
        print(f"Read {len(df)} historical records from {csv_path}")
        return df
    except Exception as e:
        print(f"Warning: Could not read historical CSV: {e}")
        return pd.DataFrame()

def read_daily_data():
    try:
        daily_path = f"s3://{MINIO_BUCKET}/daily/economics/interest_rate"
        fs = get_s3fs()
        
        all_files = fs.glob(f"{daily_path}/**/*.parquet")
        if not all_files:
            print(f"No daily data found at {daily_path}")
            return pd.DataFrame()
        
        dfs = []
        for file in all_files:
            try:
                with fs.open(file, 'rb') as f:
                    df = pd.read_parquet(f)
                dfs.append(df)
            except:
                pass
        
        if dfs:
            daily_df = pd.concat(dfs, ignore_index=True)
            print(f"Read {len(daily_df)} daily records from {len(all_files)} files")
            return daily_df
        return pd.DataFrame()
    except Exception as e:
        print(f"Warning: Could not read daily data: {e}")
        return pd.DataFrame()

def standardize_source(source):
    source_lower = str(source).lower()
    if 'sbv' in source_lower or 'gov' in source_lower or 'vn' in source_lower or 'bank' in source_lower:
        return 'Viet Nam Central Bank'
    return source

def parse_date(date_str):
    """Parse date from dd/mm/yyyy or yyyy-mm-dd format"""
    try:
        date_str = str(date_str).strip()
        
        if '-' in date_str and len(date_str) == 10:
            # Already yyyy-mm-dd format
            return date_str
        elif '/' in date_str:
            # dd/mm/yyyy format
            date_obj = datetime.strptime(date_str, '%d/%m/%Y')
            return date_obj.strftime('%Y-%m-%d')
        else:
            return None
    except:
        return None

def clean_interest_rate(df):
    """Clean interest rate data"""
    if len(df) == 0:
        return df
    
    cleaned_rows = []
    
    for idx, row in df.iterrows():
        try:
            date_str = str(row.get('date', '')).strip()
            term_str = str(row.get('term', 'Unknown')).strip()
            rate_value = row.get('interest_rate')
            volume = row.get('volume')
            source = str(row.get('source', 'Unknown')).strip()
            
            # Skip empty rows
            if not date_str or not term_str:
                continue
            
            # Skip duplicates already added
            if any(r['date'] == date_str and r['symbol'] == term_str for r in cleaned_rows):
                continue
            
            # Parse date
            date = parse_date(date_str)
            if date is None:
                continue
            
            # Convert interest rate to float
            try:
                if isinstance(rate_value, str):
                    rate = float(rate_value.replace(',', '.').replace('%', '').strip())
                else:
                    rate = float(rate_value) if pd.notna(rate_value) else None
            except:
                rate = None
            
            if rate is None or rate < 0 or rate > 100:
                continue
            
            # Convert volume to float
            try:
                if isinstance(volume, str):
                    vol = float(volume.replace(',', '').replace('.', '').strip())
                else:
                    vol = float(volume) if pd.notna(volume) else None
            except:
                vol = None
            
            # Map Vietnamese term to English and symbol
            if term_str in TERM_MAPPING:
                term_en, symbol = TERM_MAPPING[term_str]
            else:
                term_en = term_str
                symbol = term_str
            
            # Standardize source
            source_clean = standardize_source(source)
            
            cleaned_rows.append({
                'date': date,
                'term': term_en,
                'symbol': symbol,
                'interest_rate': rate,
                'volume': vol,
                'source': source_clean
            })
        except Exception as e:
            continue
    
    return pd.DataFrame(cleaned_rows)

def main():
    print("Starting interest rate cleaning job...")
    
    df_historical = read_historical_csv()
    df_daily = read_daily_data()
    
    df_combined = pd.concat([df_historical, df_daily], ignore_index=True)
    
    if len(df_combined) == 0:
        print("No data to process")
        return
    
    print(f"Total records before cleaning: {len(df_combined)}")
    
    interest_rate_clean = clean_interest_rate(df_combined)
    
    if len(interest_rate_clean) == 0:
        print("No valid data after cleaning")
        return
    
    interest_rate_clean = interest_rate_clean.drop_duplicates(subset=['date', 'symbol']).reset_index(drop=True)
    interest_rate_clean = interest_rate_clean.sort_values(['date', 'symbol']).reset_index(drop=True)
    
    print(f"Total records after cleaning: {len(interest_rate_clean)}")
    print("\nFirst 10 records:")
    print(interest_rate_clean.head(10))
    print("\nLast 10 records:")
    print(interest_rate_clean.tail(10))
    
    interest_rate_clean['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    try:
        delta_path = f"s3://{MINIO_BUCKET}/indicators/economy/interest_rate"
        
        write_deltalake(
            delta_path,
            interest_rate_clean,
            mode='append',
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        
        print(f"\nSuccessfully wrote {len(interest_rate_clean)} records to Silver Delta table")
    except Exception as e:
        print(f"Error writing to Delta: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
