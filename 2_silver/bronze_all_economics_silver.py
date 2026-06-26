"""
Silver Layer: Economic Indicators Cleaning
Process all economic indicators from Bronze to Silver
- M2, Core Inflation, CPI, PPI
- Output: s3://silver/economics/{indicator}/processing_date={date}/
"""

import os
import sys
import pandas as pd
import numpy as np
import re
from datetime import datetime
from deltalake import write_deltalake

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

def read_s3_csv(path):
    fs = get_s3fs()
    with fs.open(path, 'rb') as f:
        return pd.read_csv(f)

def read_local_csv(path):
    """Read local CSV file for testing/dev"""
    try:
        return pd.read_csv(path)
    except:
        return None

# ===== M2 PROCESSING =====
def process_m2(df=None, use_local=True):
    """Process M2 Money Supply data"""
    if df is None:
        if use_local:
            df = read_local_csv('./historical_dataset/money_supply_m2.csv')
        if df is None:
            return None
    
    def standardize_source(source):
        source_lower = str(source).lower()
        if 'funan' in source_lower:
            return 'Funan Research Institute'
        return source

    def parse_month(month_str):
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
            
            date = parse_month(month_str)
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
    
    if not m2_rows:
        return None
    
    m2_df = pd.DataFrame(m2_rows)
    m2_df = m2_df.sort_values('date').reset_index(drop=True)
    return m2_df

# ===== CORE INFLATION PROCESSING =====
def process_core_inflation(df=None):
    """Process Core Inflation data"""
    if df is None:
        try:
            bronze_path = f"s3://{MINIO_BUCKET}/historical/economics/vietnam-core-inflation-rate.csv"
            df = read_s3_csv(bronze_path)
        except:
            return None
    
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
        return cir_df
    return None

# ===== CPI PROCESSING =====
# def process_cpi(df=None):
#     """Process CPI data"""
#     if df is None:
#         try:
#             bronze_path = f"s3://{MINIO_BUCKET}/historical/economics/cpi.csv"
#             fs = get_s3fs()
#             with fs.open(bronze_path, 'rb') as f:
#                 df = pd.read_csv(f, encoding='cp1258')
#         except:
#             return None
    
#     df.rename(columns={df.columns[0]: 'month'}, inplace=True)
#     years = df.columns[1:].tolist()
    
#     cpi_mom_rows = []
#     for idx, row in df.iterrows():
#         match = re.search(r'(\d{1,2})\s*$', str(row['month']))
#         if match:
#             month_num = int(match.group(1))
#             if 1 <= month_num <= 12:
#                 for i, year in enumerate(years):
#                     if i < len(row) - 1 and pd.notnull(row[year]) and row[year] not in ['..', '...']:
#                         try:
#                             date = f"{year}-{month_num:02d}-01"
#                             cpi = float(str(row[year]).replace(',', '.'))
#                             cpi_mom_rows.append({
#                                 'date': date,
#                                 'indicator': 'cpi_mom',
#                                 'year': int(year),
#                                 'month': month_num,
#                                 'cpi': cpi,
#                                 'unit': 'points'
#                             })
#                         except:
#                             continue
    
#     if cpi_mom_rows:
#         cpi_mom_df = pd.DataFrame(cpi_mom_rows).sort_values(['year', 'month']).reset_index(drop=True)
#         cpi_mom_df['inflation'] = cpi_mom_df.groupby('year')['cpi'].transform(lambda x: x.diff() / x.shift(1) * 100)
#         cpi_mom_df['inflation'] = cpi_mom_df['inflation'].bfill()
#         cpi_mom_df['avg_year'] = cpi_mom_df.groupby('year')['cpi'].transform('mean')
#         cpi_mom_output = cpi_mom_df[['date', 'indicator', 'cpi', 'inflation', 'avg_year', 'unit']]
        
#         def get_row_by_label(label):
#             matches = df[df['month'].str.contains(label, case=False, na=True)]
#             return matches.iloc[0] if len(matches) > 0 else None
        
#         row_prev_year = get_row_by_label('=100')
#         row_2000 = get_row_by_label('Nam 2000')
#         row_2005 = get_row_by_label('Nam 2005')
#         row_2010 = get_row_by_label('Nam 2010')
        
#         cpi_base_year_rows = []
#         for i, year in enumerate(years):
#             try:
#                 prev_year_base = row_prev_year[year] if row_prev_year is not None else None
#                 base_2000 = row_2000[year] if row_2000 is not None else None
#                 base_2005 = row_2005[year] if row_2005 is not None else None
#                 base_2010 = row_2010[year] if row_2010 is not None else None
                
#                 cpi_base_year_rows.append({
#                     'date': f"{year}-01-01",
#                     'indicator': 'cpi_base_year',
#                     'prev_year_base': float(str(prev_year_base).replace('..', 'nan').replace(',', '.')) if prev_year_base and prev_year_base not in ['..', '...'] else np.nan,
#                     'base_2000': float(str(base_2000).replace('..', 'nan').replace(',', '.')) if base_2000 and base_2000 not in ['..', '...'] else np.nan,
#                     'base_2005': float(str(base_2005).replace('..', 'nan').replace(',', '.')) if base_2005 and base_2005 not in ['..', '...'] else np.nan,
#                     'base_2010': float(str(base_2010).replace('..', 'nan').replace(',', '.')) if base_2010 and base_2010 not in ['..', '...'] else np.nan,
#                     'unit': 'points'
#                 })
#             except:
#                 continue
        
#         if cpi_base_year_rows:
#             cpi_base_year_df = pd.DataFrame(cpi_base_year_rows)
#             if len(cpi_mom_rows) > 0 and len(cpi_base_year_df) > 0:
#                 annual_avg = cpi_mom_df[['year', 'avg_year']].drop_duplicates().reset_index(drop=True)
#                 annual_avg['prev_year_avg_shift'] = annual_avg['avg_year'].shift(1)
#                 annual_avg['prev_year_base_calc'] = (annual_avg['avg_year'] / annual_avg['prev_year_avg_shift']) * 100
#                 null_mask = cpi_base_year_df['prev_year_base'].isna()
#                 cpi_base_year_df.loc[null_mask, 'prev_year_base'] = cpi_base_year_df.loc[null_mask, 'date'].str[:4].astype(int).map(dict(zip(annual_avg['year'].astype(int), annual_avg['prev_year_base_calc'])))
            
#             for col in ['prev_year_base', 'base_2000', 'base_2005', 'base_2010']:
#                 valid_vals = cpi_base_year_df[col].dropna()
#                 if len(valid_vals) > 0:
#                     first_valid = valid_vals.iloc[0]
#                     cpi_base_year_df.loc[cpi_base_year_df[col].isna(), col] = first_valid
#                 else:
#                     cpi_base_year_df.loc[cpi_base_year_df[col].isna(), col] = 100.0
            
#             return cpi_base_year_df
#     return None
def process_cpi(df=None):
    """Process CPI data - returns both cpi_mom and cpi_base_year tables"""
    if df is None:
        try:
            bronze_path = f"s3://{MINIO_BUCKET}/historical/economics/cpi.csv"
            fs = get_s3fs()
            with fs.open(bronze_path, 'rb') as f:
                df = pd.read_csv(f, encoding='cp1258')
        except:
            return None
    
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
            
            # Return both tables as list of tuples
            return [('cpi_mom', cpi_mom_output), ('cpi_base_year', cpi_base_year_df)]
    return None

# ===== PPI PROCESSING =====
def process_ppi(df=None):
    """Process PPI data"""
    if df is None:
        try:
            bronze_path = f"s3://{MINIO_BUCKET}/historical/economics/vietnam-producer-price-inflation-qoq.csv"
            df = read_s3_csv(bronze_path)
        except:
            return None
    
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
        return ppi_df
    return None

def write_to_delta(indicator_df, indicator_name, processing_date):
    """Write indicator data to Delta Lake format"""
    try:
        df_to_write = indicator_df.copy()
        df_to_write['processing_date'] = processing_date
        MINIO_SILVER = os.getenv("MINIO_SILVER", "silver")
        delta_path = f"s3://{MINIO_SILVER}/economics/{indicator_name}"
        write_deltalake(
            delta_path,
            df_to_write,
            mode='overwrite',
            partition_by=['processing_date'],
            storage_options=DELTA_STORAGE_OPTIONS
        )
        print(f"  ✓ Delta Lake: {len(df_to_write)} records")
        return True
    except Exception as e:
        print(f"  ✗ Delta Lake Error: {e}")
        return False

def write_to_csv(indicator_df, indicator_name, processing_date):
    """Write indicator data to CSV format (S3)"""
    try:
        df_to_write = indicator_df.copy()
        df_to_write['processing_date'] = processing_date
        
        MINIO_SILVER = os.getenv("MINIO_SILVER", "silver")
        csv_path = f"s3://{MINIO_SILVER}/economics/{indicator_name}/processing_date={processing_date}/{indicator_name}.csv"
        fs = get_s3fs()
        
        with fs.open(csv_path, 'wb') as f:
            df_to_write.to_csv(f, index=False, encoding='utf-8')
        print(f"  ✓ CSV (S3): {len(df_to_write)} records")
        return True
    except Exception as e:
        print(f"  ✗ CSV Error: {e}")
        return False

def main():
    all_indicators = []
    
    # M2
    try:
        print("M2: Reading...")
        m2_df = process_m2()
        if m2_df is not None:
            all_indicators.append(('m2', m2_df))
            print(f"M2: {len(m2_df)} records")
            print(m2_df.head())
    except Exception as e:
        print(f"M2: {e}")
    
    # Core Inflation
    try:
        print("Core Inflation: Reading...")
        cir_df = process_core_inflation()
        if cir_df is not None:
            all_indicators.append(('core_inflation_rate', cir_df))
            print(f"Core Inflation: {len(cir_df)} records")
            print(cir_df.head())
    except Exception as e:
        print(f"Core Inflation: {e}")
    
    # CPI
    try:
        print("CPI: Reading...")
        cpi_results = process_cpi()
        if cpi_results is not None:
            # cpi_results là list của 2 tuples: [('cpi_mom', df_mom), ('cpi_base_year', df_base)]
            for indicator_name, cpi_df in cpi_results:
                if cpi_df is not None and not cpi_df.empty:
                    all_indicators.append((indicator_name, cpi_df))
                    print(f"CPI {indicator_name}: {len(cpi_df)} records")
                    print(cpi_df.head())
    except Exception as e:
        print(f"CPI: {e}")
    
    # PPI
    try:
        print("PPI: Reading...")
        ppi_df = process_ppi()
        if ppi_df is not None:
            all_indicators.append(('ppi_qoq', ppi_df))
            print(f"PPI: {len(ppi_df)} records")
            print(ppi_df.head())
    except Exception as e:
        print(f"PPI: {e}")
    
    if all_indicators:
        processing_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        
        for indicator_name, indicator_df in all_indicators:
            if not indicator_df.empty:
                print(f"{indicator_name.upper()}:")
                # Write to Delta Lake
                write_to_delta(indicator_df, indicator_name, processing_date)
                # Write to CSV
                write_to_csv(indicator_df, indicator_name, processing_date)
    else:
        print("No indicators processed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
