"""
Silver Layer: Interest Rate Cleaning
Read from Bronze (historical CSV + daily scraped data), clean, and append to Silver
- Input: s3://bronze/historical/economics/interest_rate.csv (historical)
         s3://bronze/daily/economics/interest_rate/ (daily scraped)
- Output: s3://silver/economics/interest_rate/processing_date={date}/
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
        raw_path = f"s3://{MINIO_BUCKET}/daily/economics/interest_rate"
        fs = get_s3fs()
        
        all_files = fs.glob(f"{raw_path}/**/*.parquet")
        if not all_files:
            print(f"No daily data found at {raw_path}")
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
    """Standardize source field"""
    source_lower = str(source).lower()
    if 'sbv' in source_lower or 'gov' in source_lower or 'vn' in source_lower:
        return 'Viet Nam Central Bank'
    return source

def parse_date(date_str):
    """Parse date in multiple formats and return YYYY-MM-DD"""
    date_str = str(date_str).strip()
    try:
        # Check if already in YYYY-MM-DD format (year starts with 19 or 20)
        if '-' in date_str and len(date_str) == 10:
            parts = date_str.split('-')
            if len(parts) == 3 and parts[0].isdigit() and int(parts[0]) >= 1900:
                return date_str  # Already in YYYY-MM-DD
        
        # Try DD/MM/YYYY format
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = parts
                return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
        
        # Try DD-MM-YYYY format
        if '-' in date_str:
            date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            return date_obj.strftime('%Y-%m-%d')
    except:
        pass
    
    return None

# Term mapping: Vietnamese -> English with symbol
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

def normalize_term(term_str):
    """Normalize term to English name and symbol"""
    term_str = str(term_str).strip()
    
    if term_str in TERM_MAPPING:
        return TERM_MAPPING[term_str]
    
    # Check if already in English
    if any(eng_term in term_str for eng_term, _ in TERM_MAPPING.values()):
        return (term_str, term_str)
    
    # Default fallback
    return (term_str, term_str)

def clean_interest_rate(df):
    """Clean and standardize interest rate data"""
    if len(df) == 0:
        return df
    
    # Fill null term with 'Unknown'
    df['term'] = df['term'].fillna('Unknown')
    
    cleaned_rows = []
    
    for idx, row in df.iterrows():
        try:
            date_str = str(row.get('date', '')).strip()
            term_str = str(row.get('term', '')).strip()
            rate_str = str(row.get('interest_rate', '')).strip()
            volume = row.get('volume')
            source = str(row.get('source', 'Unknown')).strip()
            
            # Skip empty rows
            if not date_str or not term_str or not rate_str:
                continue
            
            # Parse date
            date = parse_date(date_str)
            if date is None:
                continue
            
            # Check for duplicates
            if any(r['date'] == date and r['term'] == term_str and r['source'] == source for r in cleaned_rows):
                continue
            
            # Convert rate to float
            try:
                rate = float(rate_str.replace(',', '.').strip())
            except:
                continue
            
            # Skip invalid rates
            if rate < 0 or rate > 100:
                continue
            
            # Normalize term
            term_en, symbol = normalize_term(term_str)
            
            # Standardize source
            source_clean = standardize_source(source)
            
            cleaned_rows.append({
                'date': date,
                'term': term_en,
                'symbol': symbol,
                'interest_rate': rate,
                'volume': volume if pd.notna(volume) else None,
                'source': source_clean
            })
        except Exception as e:
            continue
    
    return pd.DataFrame(cleaned_rows)
def write_to_csv(interest_rate_df, processing_date):
    """Write interest rate data to CSV format (S3)"""
    try:
        df_to_write = interest_rate_df.copy()
        
        fs = get_s3fs()
        csv_path = f"s3://silver/economics/interest_rate/processing_date={processing_date}/interest_rate.csv"
        
        with fs.open(csv_path, 'wb') as f:
            df_to_write.to_csv(f, index=False, encoding='utf-8')
        print(f"  ✓ CSV (S3): {len(df_to_write)} records written to {csv_path}")
        return True
    except Exception as e:
        print(f"  ✗ CSV Error: {e}")
        return False
def main():
    print("Starting interest rate cleaning job...")
    
    df_historical = read_historical_csv()
    df_daily = read_daily_data()
    
    df_combined = pd.concat([df_historical, df_daily], ignore_index=True)
    
    if len(df_combined) == 0:
        print("No data to process")
        return
    
    print(f"Total records before cleaning: {len(df_combined)}")
    print(f"Null values before cleaning:")
    print(df_combined.isnull().sum())
    
    # Clean interest rates
    interest_rate_clean = clean_interest_rate(df_combined)
    
    if len(interest_rate_clean) == 0:
        print("No valid data after cleaning")
        return
    
    # Remove duplicates and sort
    interest_rate_clean = interest_rate_clean.drop_duplicates(
        subset=['date', 'term', 'symbol', 'source']
    ).reset_index(drop=True)
    
    interest_rate_clean = interest_rate_clean.sort_values(
        ['date', 'symbol']
    ).reset_index(drop=True)
    
    print(f"Total records after cleaning: {len(interest_rate_clean)}")
    print("\nFirst 20 records:")
    print(interest_rate_clean.head(20))
    print("\nLast 10 records:")
    print(interest_rate_clean.tail(10))
    
    # Add processing date
    interest_rate_clean['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    try:
        delta_path = f"s3://silver/economics/interest_rate"
        
        write_deltalake(
            delta_path,
            interest_rate_clean,
            mode='overwrite',
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        
        print(f"\nSuccessfully wrote {len(interest_rate_clean)} records to Silver Delta table")

        processing_date = interest_rate_clean['processing_date'].iloc[0]
        write_to_csv(interest_rate_clean, processing_date)
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
