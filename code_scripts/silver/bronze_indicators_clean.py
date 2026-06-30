import os
import sys
import pandas as pd
import numpy as np
import re
from datetime import datetime
from deltalake import write_deltalake, DeltaTable

import s3fs

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

STORAGE_OPTIONS = {
    "key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
}

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}

def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )

def read_s3_csv(path):
    fs = get_s3fs()
    with fs.open(path, 'rb') as f:
        return pd.read_csv(f)


def main():
    all_indicators = []
    
    try:
        print("Core Inflation: Reading...")
        bronze_path = f"{MINIO_BUCKET}/historical/economics/vietnam-core-inflation-rate.csv"
        df = read_s3_csv(bronze_path)
        
        def parse_month(month_str):
            parts = month_str.replace('Thg', '').split('-')
            month = int(parts[0])
            year = int(parts[1])
            if year >= 0 and year <= 30:
                year = 2000 + year
            return f"{year:04d}-{month:02d}-01"
        
        rows = []
        for idx, row in df.iterrows():
            try:
                date = parse_month(row['MONTH'].strip())
                rate = float(row['CORE INFLATION RATE'].strip().replace('%', ''))
                rows.append({
                    'date': date,
                    'indicator': 'core_inflation_rate',
                    'value': rate,
                    'unit': '%'
                })
            except:
                continue
        
        if rows:
            cir_df = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
            all_indicators.append(cir_df)
            print(f"Core Inflation: {len(cir_df)} records")
            print(cir_df.head())
    except Exception as e:
        print(f"Core Inflation: {e}")
    
    # M2 Processing
    try:
        print("\nM2: Reading...")
        bronze_path = f"s3://{MINIO_BUCKET}/historical/economics/money_supply_m2.csv"
        df = read_s3_csv(bronze_path)
        
        def standardize_source(source):
            source_lower = str(source).lower()
            if 'funan' in source_lower:
                return 'Funan Research Institute'
            return source

        def parse_month_m2(month_str):
            # "Tháng 1/2026" -> "2026-01-01"
            try:
                parts = month_str.replace('Tháng ', '').split('/')
                month = int(parts[0])
                year = int(parts[1])
                return f"{year:04d}-{month:02d}-01"
            except:
                return None

        df['source'] = df['source'].apply(standardize_source)
        
        m2_rows = []
        for idx, row in df.iterrows():
            try:
                month_str = row['month'].strip()
                m2_value = row['m2']
                unit = row['unit'].strip()
                source = row['source'].strip()
                
                date = parse_month_m2(month_str)
                if date is None:
                    continue
                
                m2_rows.append({
                    'date': date,
                    'indicator': 'm2',
                    'value': m2_value,
                    'unit': unit,
                    'source': source
                })
            except:
                continue
        
        if m2_rows:
            m2_df = pd.DataFrame(m2_rows)
            m2_df = m2_df.sort_values('date').reset_index(drop=True)
            all_indicators.append(m2_df)
            print(f"M2: {len(m2_df)} records")
            print(m2_df.head())
    except Exception as e:
        print(f"M2: {e}")
        bronze_path = f"{MINIO_BUCKET}/historical/economics/cpi.csv"
        fs = get_s3fs()
        with fs.open(bronze_path, 'rb') as f:
            df = pd.read_csv(f, encoding='cp1258')
        df.rename(columns={df.columns[0]: 'month'}, inplace=True)
        
        years = df.columns[1:].tolist()
        
        cpi_mom_rows = []
        for idx, row in df.iterrows():
            match = re.search(r'(\d{1,2})\s*$', str(row['month']))
            if match:
                month_num = int(match.group(1))
                if 1 <= month_num <= 12:
                    for i, year in enumerate(years):
                        if i < len(row) - 1 and pd.notnull(row[year]) and row[year] not in ['..', '...']:
                            try:
                                date = f"{year}-{month_num:02d}-01"
                                cpi = float(str(row[year]).replace(',', '.'))
                                cpi_mom_rows.append({
                                    'date': date,
                                    'indicator': 'cpi_mom',
                                    'year': int(year),
                                    'month': month_num,
                                    'cpi': cpi,
                                    'unit': 'points'
                                })
                            except:
                                continue
        
        if cpi_mom_rows:
            cpi_mom_df = pd.DataFrame(cpi_mom_rows).sort_values(['year', 'month']).reset_index(drop=True)
            cpi_mom_df['inflation'] = cpi_mom_df.groupby('year')['cpi'].transform(lambda x: x.diff() / x.shift(1) * 100)
            cpi_mom_df['inflation'] = cpi_mom_df['inflation'].bfill()
            cpi_mom_df['avg_year'] = cpi_mom_df.groupby('year')['cpi'].transform('mean')
            cpi_mom_output = cpi_mom_df[['date', 'indicator', 'cpi', 'inflation', 'avg_year', 'unit']]
            all_indicators.append(cpi_mom_output)
            print(f"CPI Mom: {len(cpi_mom_output)} records")
            print(cpi_mom_output.head())
        def get_row_by_label(label):
            matches = df[df['month'].str.contains(label, case=False, na=True)]
            return matches.iloc[0] if len(matches) > 0 else None
        
        row_prev_year = get_row_by_label('=100')
        row_2000 = get_row_by_label('Nam 2000')
        row_2005 = get_row_by_label('Nam 2005')
        row_2010 = get_row_by_label('Nam 2010')
        
        cpi_base_year_rows = []
        for i, year in enumerate(years):
            try:
                prev_year_base = row_prev_year[year] if row_prev_year is not None else None
                base_2000 = row_2000[year] if row_2000 is not None else None
                base_2005 = row_2005[year] if row_2005 is not None else None
                base_2010 = row_2010[year] if row_2010 is not None else None
                
                cpi_base_year_rows.append({
                    'date': f"{year}-01-01",
                    'indicator': 'cpi_base_year',
                    'prev_year_base': float(str(prev_year_base).replace('..', 'nan').replace(',', '.')) if prev_year_base and prev_year_base not in ['..', '...'] else np.nan,
                    'base_2000': float(str(base_2000).replace('..', 'nan').replace(',', '.')) if base_2000 and base_2000 not in ['..', '...'] else np.nan,
                    'base_2005': float(str(base_2005).replace('..', 'nan').replace(',', '.')) if base_2005 and base_2005 not in ['..', '...'] else np.nan,
                    'base_2010': float(str(base_2010).replace('..', 'nan').replace(',', '.')) if base_2010 and base_2010 not in ['..', '...'] else np.nan,
                    'unit': 'points'
                })
            except:
                continue
        
        if cpi_base_year_rows:
            cpi_base_year_df = pd.DataFrame(cpi_base_year_rows)
            if len(cpi_mom_rows) > 0 and len(cpi_base_year_df) > 0:
                annual_avg = cpi_mom_df[['year', 'avg_year']].drop_duplicates().reset_index(drop=True)
                annual_avg['prev_year_avg_shift'] = annual_avg['avg_year'].shift(1)
                annual_avg['prev_year_base_calc'] = (annual_avg['avg_year'] / annual_avg['prev_year_avg_shift']) * 100
                null_mask = cpi_base_year_df['prev_year_base'].isna()
                cpi_base_year_df.loc[null_mask, 'prev_year_base'] = cpi_base_year_df.loc[null_mask, 'date'].str[:4].astype(int).map(dict(zip(annual_avg['year'].astype(int), annual_avg['prev_year_base_calc'])))
            
            for col in ['prev_year_base', 'base_2000', 'base_2005', 'base_2010']:
                valid_vals = cpi_base_year_df[col].dropna()
                if len(valid_vals) > 0:
                    first_valid = valid_vals.iloc[0]
                    cpi_base_year_df.loc[cpi_base_year_df[col].isna(), col] = first_valid
                else:
                    cpi_base_year_df.loc[cpi_base_year_df[col].isna(), col] = 100.0
            
            all_indicators.append(cpi_base_year_df)
            print(f"CPI Base Year: {len(cpi_base_year_df)} records")
            print(cpi_base_year_df.head())  
    except Exception as e:
        print(f"CPI: {e}")
    
    try:
        print("PPI: Reading...")
        bronze_path = f"{MINIO_BUCKET}/historical/economics/vietnam-producer-price-inflation-qoq.csv"
        df = read_s3_csv(bronze_path)
        
        def extract_quarter_to_date(quarter_str):
            parts = quarter_str.split()
            q = int(parts[0][1])
            year = int(parts[1])
            month = (q - 1) * 3 + 1
            return f"{year:04d}-{month:02d}-01"
        
        rows = []
        for idx, row in df.iterrows():
            try:
                date = extract_quarter_to_date(row['QUARTER'].strip())
                ppi_value = float(row['PRODUCER PRICE INFLATION QOQ'].strip().replace('%', ''))
                rows.append({
                    'date': date,
                    'indicator': 'ppi_qoq',
                    'value': ppi_value,
                    'unit': '%'
                })
            except:
                continue
        
        if rows:
            ppi_df = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
            all_indicators.append(ppi_df)
            print(f"PPI: {len(ppi_df)} records")
            print(ppi_df.head())
    except Exception as e:
        print(f"PPI: {e}")
    
    if all_indicators:
        processing_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        for indicator_df in all_indicators:
            if not indicator_df.empty:
                indicator_name = indicator_df.iloc[0]['indicator'] if 'indicator' in indicator_df.columns else 'unknown'
                
                df_to_write = indicator_df.copy()
                df_to_write['processing_date'] = processing_date
                MINIO_SILVER = os.getenv("MINIO_SILVER", "silver")
                delta_path = f"s3://{MINIO_SILVER}/economics/{indicator_name}"
                try:
                    write_deltalake(
                        delta_path,
                        df_to_write,
                        mode='overwrite',
                        partition_by=['processing_date'],
                        storage_options=DELTA_STORAGE_OPTIONS
                    )
                    print(f"{indicator_name}: {len(df_to_write)} records written")
                except Exception as e:
                    print(f"{indicator_name} Error: {e}")
    else:
        print("No indicators processed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
