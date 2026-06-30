"""
Silver Layer: Vietnam OHLC Data Cleaning - Dual Source
- Source 1: CSV files (historical, if available)
- Source 2: Delta tables from Bronze raw (realtime data) - NEW
- Aggregate OHLC from realtime records → append to Silver
- Output: s3://silver/vietnam_index/historical/{symbol}/ (append mode)
"""

import os
import yaml
import pandas as pd
import io
from datetime import datetime
from minio import Minio
from deltalake import DeltaTable, write_deltalake
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET", "bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}
endpoint_host = STORAGE_OPTIONS["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
minio_client = Minio(
    endpoint=endpoint_host,
    access_key=STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"],
    secret_key=STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"],
    secure=False
)

CSV_COLUMN_MAP_OHLC = {
    'date':           'Ngày',
    'open':           'Mở',
    'high':           'Cao',
    'low':            'Thấp',
    'close':          'Lần cuối',
    'volume':         'KL',
    'change_percent': '% Thay đổi',
}

def _parse_date(date_str):
    """Parse date string to YYYY-MM-DD format"""
    try:
        return datetime.strptime(str(date_str).strip(), '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

def _parse_price(val):
    """Parse price value, handling Vietnamese number format"""
    try:
        return float(str(val).strip().replace(',', ''))
    except:
        return None

def _convert_volume(val):
    """Convert volume with K, M, B suffixes to actual number"""
    if pd.isnull(val) or str(val).upper() == 'NAN':
        return 0
    s = str(val).strip().upper()
    for suffix, mult in [('B', 1_000_000_000), ('M', 1_000_000), ('K', 1_000)]:
        if s.endswith(suffix):
            try:
                return float(s[:-1]) * mult
            except:
                return 0
    try:
        return float(s)
    except:
        return None

def read_csv_from_s3(s3_path: str) -> pd.DataFrame:
    """Read CSV file from S3 (MinIO) using Minio client"""
    try:
        # Parse S3 path
        if s3_path.startswith('s3://'):
            bucket, key = s3_path[5:].split('/', 1)
        else:
            bucket, key = s3_path.split('/', 1)
        
        logger.info(f"Reading from S3: s3://{bucket}/{key}")
        
        # Read from MinIO using minio_client
        response = minio_client.get_object(bucket, key)
        csv_data = response.read()
        df = pd.read_csv(io.BytesIO(csv_data))
        
        logger.info(f"Successfully read {len(df)} rows from S3")
        return df
    
    except Exception as e:
        logger.error(f"Error reading from S3 path {s3_path}: {e}", exc_info=True)
        raise

def read_raw_from_bronze_delta(symbol: str) -> pd.DataFrame:
    """Read raw realtime data from Delta table in Bronze layer
    Path: s3://bronze/raw/vietnam_index/processing_date=YYYY-MM-DD/...
    """
    try:
        bronze_path = f"s3://{MINIO_BUCKET_BRONZE}/raw/vietnam_index"
        logger.info(f"Reading Delta table from: {bronze_path}")
        
        # Read Delta table
        dt = DeltaTable(bronze_path, storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas()
        
        if df.empty:
            logger.warning(f"No data in Delta table for {symbol}")
            return df
        
        # Filter by index symbol (column name is 'index', not 'symbol')
        if 'index' in df.columns:
            df = df[df['index'] == symbol].copy()
        elif 'symbol' in df.columns:
            df = df[df['symbol'] == symbol].copy()
        else:
            logger.error(f"Neither 'index' nor 'symbol' column found. Available: {df.columns.tolist()}")
            return pd.DataFrame()
        
        if df.empty:
            logger.warning(f"No records for {symbol} in Delta table")
            return df
        
        logger.info(f"Read {len(df)} raw records for {symbol} from Bronze Delta table")
        logger.info(f"Columns: {df.columns.tolist()}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading Delta table from Bronze for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()
def write_to_csv(df: pd.DataFrame, symbol: str):
    """Write Vietnam OHLC data to CSV format (S3)"""
    try:
        import s3fs
        
        processing_date = datetime.utcnow().strftime('%Y-%m-%d')
        csv_path = f"s3://{MINIO_BUCKET_SILVER}/vietnam_index/{symbol}/processing_date={processing_date}/{symbol}.csv"
        
        fs = s3fs.S3FileSystem(
            key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            use_ssl=False,
        )
        
        with fs.open(csv_path, 'wb') as f:
            df.to_csv(f, index=False, encoding='utf-8')
        
        logger.info(f"  ✓ CSV written: {len(df)} records to {csv_path}")
        return True
    except Exception as e:
        logger.error(f"  ✗ CSV Error: {e}")
        return False
def aggregate_ohlc_from_raw(df_raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Aggregate OHLC from raw realtime data (multiple records per day or per minute)
    Assumes df_raw has 'price', 'timestamp' (or 'ingestion_timestamp') columns
    """
    try:
        if df_raw.empty:
            logger.warning(f"No data to aggregate for {symbol}")
            return pd.DataFrame()
        
        logger.info(f"Aggregating OHLC from {len(df_raw)} raw records for {symbol}")
        logger.info(f"Available columns: {df_raw.columns.tolist()}")
        print(f"Sample data:\n{df_raw[['timestamp', 'index', 'price', 'volume', 'change', 'percent']].head(10)}")
        
        # Ensure we have price column
        if 'price' not in df_raw.columns:
            logger.error(f"Missing 'price' column in raw data for {symbol}")
            return pd.DataFrame()
        
        # Use the timestamp column
        timestamp_col = 'timestamp'  # Based on actual data, this is the main timestamp
        if timestamp_col not in df_raw.columns:
            logger.error(f"Missing '{timestamp_col}' column in raw data for {symbol}")
            return pd.DataFrame()
        
        df = df_raw.copy()
        
        # Parse timestamp (ISO format with timezone)
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
        df['date'] = df[timestamp_col].dt.strftime('%Y-%m-%d')
        
        logger.info(f"Date range: {df['date'].min()} → {df['date'].max()}")
        logger.info(f"Records per date:\n{df['date'].value_counts().sort_index()}")
        
        # Build aggregation dict dynamically
        agg_dict = {
            'price': ['first', 'max', 'min', 'last'],
        }
        
        if 'volume' in df.columns:
            agg_dict['volume'] = 'sum'
        
        if 'change' in df.columns:
            agg_dict['change'] = 'last'
        
        if 'percent' in df.columns:
            agg_dict['percent'] = 'last'
        
        # Aggregate OHLC by date
        ohlc = df.groupby('date').agg(agg_dict).reset_index()
        
        # Flatten multi-level columns
        if isinstance(ohlc.columns, pd.MultiIndex):
            ohlc.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in ohlc.columns.values]
        
        logger.info(f"Columns after aggregation: {ohlc.columns.tolist()}")
        
        # Rename columns to match output format
        col_mapping = {
            'price_first': 'open',
            'price_max': 'high',
            'price_min': 'low',
            'price_last': 'close',
            'volume_sum': 'volume',
            'change_last': 'change',
            'percent_last': 'percent',
        }
        
        # Rename only columns that exist
        for old_name, new_name in col_mapping.items():
            if old_name in ohlc.columns:
                ohlc.rename(columns={old_name: new_name}, inplace=True)
        
        logger.info(f"Columns after rename: {ohlc.columns.tolist()}")
        
        # Calculate change_percent
        if 'percent' in ohlc.columns:
            ohlc['change_percent'] = ohlc['percent'].fillna(0)
        else:
            ohlc['change_percent'] = ((ohlc['close'] - ohlc['open']) / ohlc['open'] * 100).fillna(0)
        
        # Ensure all required columns exist
        if 'volume' not in ohlc.columns:
            ohlc['volume'] = 0
        if 'change' not in ohlc.columns:
            ohlc['change'] = 0
        
        # Add metadata
        ohlc['symbol']      = symbol
        ohlc['asset_class'] = 'vietnam_index'
        ohlc['unit']        = 'point'
        ohlc['source']      = 'realtime'
        
        # Select and order columns
        final_columns = ['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 'volume', 'change_percent', 'change', 'source']
        
        
        
        ohlc = ohlc[final_columns]
        ohlc = ohlc.sort_values('date').reset_index(drop=True)
        
        # Type conversion
        for col in ['open', 'high', 'low', 'close', 'volume', 'change_percent', 'change']:
            ohlc[col] = pd.to_numeric(ohlc[col], errors='coerce').astype('float64')
        
        ohlc['symbol'] = ohlc['symbol'].astype(str)
        ohlc['asset_class'] = ohlc['asset_class'].astype(str)
        ohlc['unit'] = ohlc['unit'].astype(str)
        ohlc['source'] = ohlc['source'].astype(str)
        
        logger.info(f"Aggregated {len(ohlc)} OHLC records for {symbol}")
        print(f"Final OHLC:\n{ohlc.to_string()}")
        
        return ohlc
    
    except Exception as e:
        logger.error(f"Error aggregating OHLC for {symbol}: {e}", exc_info=True)
        print(f"DataFrame info:\n{df_raw.info()}")
        print(f"Sample data:\n{df_raw.head()}")
        return pd.DataFrame()

def clean_ohlc(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Clean and standardize OHLC data from CSV"""
    logger.info(f"Cleaning OHLC data: {len(df)} rows")
    print(f"df_raw nulls:\n{df.isnull().sum()}")
    print(f"df_raw head:\n{df.head().to_string()}")
    
    out = pd.DataFrame()
    out['date']           = df[column_map['date']].apply(_parse_date)
    out['close']          = df[column_map['close']].apply(_parse_price)
    out['open']           = df[column_map['open']].apply(_parse_price)
    out['high']           = df[column_map['high']].apply(_parse_price)
    out['low']            = df[column_map['low']].apply(_parse_price)
    out['volume']         = df[column_map['volume']].apply(_convert_volume)
    out['change_percent'] = df[column_map['change_percent']].apply(
        lambda v: float(str(v).strip().replace('%', '')) if str(v).strip().replace('%', '') not in ('nan', '') else None
    )
    
    out = out[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]
    out = out.dropna(subset=['date'])
    
    print(f"df_clean nulls:\n{out.isnull().sum()}")
    print(f"df_clean head:\n{out.head().to_string()}")
    
    logger.info(f"Cleaned {len(out)} rows")
    return out

def append_to_silver(df: pd.DataFrame, path: str):
    """Append OHLC data to Silver Delta table (deduplicate by date+symbol)"""
    try:
        if df.empty:
            logger.warning(f"No data to append to {path}")
            return
        
        try:
            # Try to read existing Silver table
            dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
            existing_df = dt.to_pandas()
            
            # Deduplicate: remove existing dates for this symbol
            symbol = df['symbol'].iloc[0]
            existing_dates = set(existing_df[existing_df['symbol'] == symbol]['date'].unique())
            df_new = df[~df['date'].isin(existing_dates)].copy()
            
            if df_new.empty:
                logger.info(f"All records already exist in {path}, skipping append")
                return
            
            # Append new records
            write_deltalake(path, df_new, mode='append', storage_options=STORAGE_OPTIONS)
            logger.info(f"Appended {len(df_new)} new records to {path}")
            write_to_csv(df_new, symbol)
        except Exception as e:
            # Table doesn't exist, create new
            logger.info(f"Creating new Delta table at {path}")
            write_deltalake(path, df, mode='overwrite', partition_by=['symbol'], storage_options=STORAGE_OPTIONS)
            logger.info(f"Created new Delta table with {len(df)} records")
    
    except Exception as e:
        logger.error(f"Error appending to Silver {path}: {e}", exc_info=True)
        raise

def process_vietnam_index_csv(symbol: str, s3_file_path: str, unit: str):
    """Process Vietnam index OHLC data from CSV file (historical)"""
    logger.info("=" * 60)
    logger.info(f"Processing Vietnam Index OHLC (CSV) for {symbol}")
    logger.info("=" * 60)
    
    try:
        df_raw = read_csv_from_s3(s3_file_path)
        df_clean = clean_ohlc(df_raw, CSV_COLUMN_MAP_OHLC)
        
        if df_clean.empty:
            logger.warning(f"No valid data after cleaning for {symbol}")
            return False
        
        df_full = df_clean.sort_values('date').reset_index(drop=True)
        df_full['change_percent'] = df_full['close'].pct_change() * 100
        df_full.loc[0, 'change_percent'] = 0
        
        df_full['symbol']      = symbol
        df_full['asset_class'] = 'vietnam_index'
        df_full['unit']        = unit
        df_full['change']      = None
        df_full['source']      = 'scraped'
        
        ohlc = df_full[['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 'volume', 'change_percent', 'change', 'source']]
        ohlc = ohlc.sort_values('date').reset_index(drop=True)
        
       
        ohlc['change'] = ohlc['change'].astype('float64')
        ohlc['symbol'] = ohlc['symbol'].astype(str)
        ohlc['asset_class'] = ohlc['asset_class'].astype(str)
        ohlc['unit'] = ohlc['unit'].astype(str)
        ohlc['source'] = ohlc['source'].astype(str)
        
        print(f"ohlc head:\n{ohlc.head().to_string()}")
        
        logger.info(f"CSV Summary for {symbol}: {len(ohlc)} rows | {ohlc['date'].min()} → {ohlc['date'].max()}")
        
        silver_path = f"s3://{MINIO_BUCKET_SILVER}/vietnam_index/{symbol}"
        append_to_silver(ohlc, silver_path)
        logger.info(f"✅ CSV Done: {len(ohlc)} rows")
        # write_to_csv(ohlc, symbol)
        return True
    except Exception as e:
        logger.error(f"Error processing CSV for {symbol}: {e}", exc_info=True)
        return False

def process_vietnam_index_bronze(symbol: str, unit: str):
    """Process Vietnam index OHLC data from Bronze Delta table (realtime aggregation) - NEW"""
    logger.info("=" * 60)
    logger.info(f"Processing Vietnam Index OHLC (Bronze Delta) for {symbol}")
    logger.info("=" * 60)
    
    try:
        # Read raw realtime data from Bronze
        df_raw = read_raw_from_bronze_delta(symbol)
        
        if df_raw.empty:
            logger.warning(f"No raw data in Bronze for {symbol}")
            return False
        
        # Aggregate to OHLC
        ohlc = aggregate_ohlc_from_raw(df_raw, symbol)
        
        if ohlc.empty:
            logger.warning(f"No aggregated OHLC for {symbol}")
            return False
        
        print(f"ohlc head:\n{ohlc.head().to_string()}")
        
        logger.info(f"Bronze Summary for {symbol}: {len(ohlc)} rows | {ohlc['date'].min()} → {ohlc['date'].max()}")
        
        # Append to Silver
        silver_path = f"s3://{MINIO_BUCKET_SILVER}/vietnam_index/{symbol}"
        append_to_silver(ohlc, silver_path)
        logger.info(f"✅ Bronze Done: {len(ohlc)} rows appended")
        
        return True
    except Exception as e:
        logger.error(f"Error processing Bronze for {symbol}: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        possible_paths = [
            os.path.join(os.path.dirname(__file__), '../ingestion/api_loaders/yaml/vn_index.yaml'),
            '/app/ingestion/api_loaders/yaml/vn_index.yaml',
            'ingestion/api_loaders/yaml/vn_index.yaml',
        ]
        
        yaml_path = None
        for path in possible_paths:
            if os.path.exists(path):
                yaml_path = path
                break
        
        if not yaml_path:
            logger.error(f"YAML config not found at any of: {possible_paths}")
            exit(1)
        
        logger.info(f"Loading configuration from {yaml_path}")
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"✅ Loaded configuration successfully\n")
        
        if 'vietnam_indices' in config and config['vietnam_indices']:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {len(config['vietnam_indices'])} Vietnam indices (CSV + Bronze Delta)...")
            logger.info(f"{'='*60}\n")
            
            processed_csv = 0
            processed_bronze = 0
            
            for vietnam_index in config['vietnam_indices']:
                symbol = vietnam_index.get('symbol')
                s3_path = vietnam_index.get('s3_path') or vietnam_index.get('local_path')
                unit = vietnam_index.get('unit', 'point')
                
                if not symbol:
                    logger.warning(f"Missing symbol for Vietnam index: {vietnam_index}")
                    continue
                
                # Process CSV if available
                if s3_path:
                    if not s3_path.startswith('s3://'):
                        s3_path = f"s3://{MINIO_BUCKET_BRONZE}/{s3_path}"
                    
                    if process_vietnam_index_csv(symbol, s3_path, unit):
                        processed_csv += 1
                
                # Process Bronze Delta (realtime aggregation)
                if process_vietnam_index_bronze(symbol, unit):
                    processed_bronze += 1
        
            logger.info(f"\n{'='*60}")
            logger.info(f"✅ Processing Complete:")
            logger.info(f"   - CSV: {processed_csv}/{len(config['vietnam_indices'])} successful")
            logger.info(f"   - Bronze Delta: {processed_bronze}/{len(config['vietnam_indices'])} successful")
            logger.info(f"{'='*60}\n")
        else:
            logger.warning("No Vietnam indices found in YAML configuration")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)