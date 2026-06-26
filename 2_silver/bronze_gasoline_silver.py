"""
Silver Layer: Gasoline Price Cleaning
Read from Bronze (historical CSV + daily scraped data), clean, and append to Silver
- Input: s3://bronze/historical/product/gasoline_prices.csv (historical)
         s3://bronze/raw/product/gasoline/ (daily scraped)
- Output: s3://bronze/indicators/economy/gasoline/processing_date={date}/
"""

import os
import sys
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime
import re
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
        csv_path = f"s3://{MINIO_BUCKET}/historical/product/gasoline_prices.csv"
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
        raw_path = f"s3://{MINIO_BUCKET}/daily/product/gasoline"
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
    source_lower = str(source).lower()
    if 'pvoil' in source_lower:
        return 'PVOIL'
    elif 'petrolimex' in source_lower:
        return 'Petrolimex'
    return source

def clean_oil_name(name):
    name = name.upper()
    name = re.sub(r'(\d+[,.]\d+%?S(-\w+)?|[- ]+[IVXLC]+$)', '', name)
    if 'XĂNG E5' in name: return 'E5 RON 92'
    if 'RON 95' in name: return 'RON 95'
    if 'DẦU DO' in name: return 'DO'
    if 'DẦU FO' in name: return 'FO'
    if 'DẦU KO' in name: return 'KO'
    return name.strip()

def clean_type(name):
    if 'Xăng' in name: return 'Gasoline'
    if 'Dầu FO' in name: return 'Fuel Oil'
    if 'Dầu KO' in name: return 'Kerosene'
    if 'Dầu' in name: return 'Diesel'
    return 'Other'

def clean_gasoline(df):
    if len(df) == 0:
        return df
    
    cleaned_rows = []
    
    for idx, row in df.iterrows():
        try:
            date_str = str(row.get('date', '')).strip()
            product = str(row.get('product', '')).strip()
            price = row.get('price')
            change = str(row.get('change', 'N/A')).strip()
            unit = str(row.get('unit', 'VND/liter')).strip()
            source = str(row.get('source', 'Unknown')).strip()
            
            # Skip empty rows
            if not date_str or not product:
                continue
            # xóa dupcate:
            if any(r['date'] == date_str and r['product'] == product and r['source'] == source for r in cleaned_rows):
                continue
            # Parse date
            try:
                if '-' in date_str and len(date_str) == 10:
                    date = date_str
                elif '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts) == 3:
                        day, month, year = parts
                        date = f"{year}-{month:0>2}-{day:0>2}"
                    else:
                        continue
                else:
                    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                    date = date_obj.strftime('%Y-%m-%d')
            except:
                continue
            
            # Convert price to float
            try:
                if isinstance(price, str):
                    price_val = float(price.replace(',', '').replace(' ', ''))
                else:
                    price_val = float(price) if pd.notna(price) else None
            except:
                price_val = None
            
            if price_val is None or price_val <= 0:
                continue
            
            # Clean fields
            product_clean = clean_oil_name(product)
            type_clean = clean_type(product)
            source_clean = standardize_source(source)
            
            cleaned_rows.append({
                'date': date,
                'type': type_clean,
                'product': product_clean,
                'price': price_val,
                'change': change,
                'unit': unit,
                'source': source_clean
            })
        except Exception as e:
            continue
    
    return pd.DataFrame(cleaned_rows)
def write_to_csv(gasoline_df, processing_date):
    """Write gasoline data to CSV format (S3)"""
    try:
        df_to_write = gasoline_df.copy()
        
        fs = get_s3fs()
        csv_path = f"s3://silver/product/gasoline/processing_date={processing_date}/gasoline.csv"
        
        with fs.open(csv_path, 'wb') as f:
            df_to_write.to_csv(f, index=False, encoding='utf-8')
        print(f"  ✓ CSV (S3): {len(df_to_write)} records written")
        return True
    except Exception as e:
        print(f"  ✗ CSV Error: {e}")
        return False

def main():
    print("Starting gasoline cleaning job...")
    
    df_historical = read_historical_csv()
    df_daily = read_daily_data()
    
    df_combined = pd.concat([df_historical, df_daily], ignore_index=True)
    
    if len(df_combined) == 0:
        print("No data to process")
        return
    
    print(f"Total records before cleaning: {len(df_combined)}")
    
    gasoline_clean = clean_gasoline(df_combined)
    
    if len(gasoline_clean) == 0:
        print("No valid data after cleaning")
        return
    
    gasoline_clean = gasoline_clean.drop_duplicates(subset=['date', 'product', 'source']).reset_index(drop=True)
    gasoline_clean = gasoline_clean.sort_values('date').reset_index(drop=True)
    
    print(f"Total records after cleaning: {len(gasoline_clean)}")
    print("\nFirst 10 records:")
    print(gasoline_clean.head(10))
    print("\nLast 10 records:")
    print(gasoline_clean.tail(10))
    
    gasoline_clean['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    
    try:
        delta_path = f"s3://silver/product/gasoline"
        
        write_deltalake(
            delta_path,
            gasoline_clean,
            mode='overwrite',
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        
        print(f"\nSuccessfully wrote {len(gasoline_clean)} records to Silver Delta table")
        
        # Write to CSV
        processing_date = gasoline_clean['processing_date'].iloc[0]
        write_to_csv(gasoline_clean, processing_date)
        
    except Exception as e:
        print(f"Error writing to Silver: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)