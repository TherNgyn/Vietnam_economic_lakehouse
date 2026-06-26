"""
Silver Layer: OHLC Data Cleaning
Process historical OHLC data from Bronze to Silver
- Clean OHLC (Open, High, Low, Close) data
- Fill missing dates from yfinance
- Output: s3://silver/{asset_class}/historical/{symbol}/
"""

import os
import io
import yaml
import pandas as pd
import yfinance as yf
from datetime import datetime
from minio import Minio
from deltalake import DeltaTable, write_deltalake
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET", "bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# CSV Column mappings for OHLC data
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

def clean_ohlc(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Clean and standardize OHLC data"""
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
    out = out.dropna(subset=['date'])  # Remove rows with invalid dates
    
    print(f"df_clean nulls:\n{out.isnull().sum()}")
    print(f"df_clean head:\n{out.head().to_string()}")
    
    logger.info(f"Cleaned {len(out)} rows")
    return out
def write_to_csv(df: pd.DataFrame, asset_class: str, symbol: str):
    """Write OHLC data to CSV format (S3)"""
    try:
        import s3fs
        
        processing_date = datetime.utcnow().strftime('%Y-%m-%d')
        csv_path = f"s3://{MINIO_BUCKET_SILVER}/{asset_class}/historical/{symbol}/processing_date={processing_date}/{symbol}.csv"
        
        # Use s3fs to write
        fs = s3fs.S3FileSystem(
            key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
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
def get_yfinance_missing(symbol: str, raw_dates: set, yf_symbol: str) -> pd.DataFrame:
    """Fill missing dates from yfinance"""
    logger.info(f"Fetching yfinance data for {symbol} ({yf_symbol})...")
    try:
        hist = yf.Ticker(yf_symbol).history(period='max', interval='1d')
        if hist.empty:
            logger.warning(f"yfinance returned empty data for {yf_symbol}")
            return pd.DataFrame()
        
        df = hist.reset_index()
        if df['Date'].dt.tz is not None:
            df['Date'] = df['Date'].dt.tz_localize(None)
        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
        df['change_percent'] = None
        df = df[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]
        
        missing = sorted(set(df['date'].unique()) - raw_dates)
        logger.info(f"yfinance missing dates for {symbol}: {len(missing)}")
        
        if missing:
            print(f"df_yf nulls:\n{df[df['date'].isin(missing)].isnull().sum()}")
            print(f"df_yf head:\n{df[df['date'].isin(missing)].head().to_string()}")
        
        return df[df['date'].isin(missing)].copy()
    except Exception as e:
        logger.warning(f"yfinance error for {symbol}: {e}")
        return pd.DataFrame()

def upsert_to_silver(df: pd.DataFrame, path: str):
    """Upsert OHLC data to Silver Delta table"""
    try:
        dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
        dt.merge(
            source=df,
            predicate="s.date = t.date AND s.symbol = t.symbol",
            source_alias='s',
            target_alias='t',
        ).when_matched_update_all().when_not_matched_insert_all().execute()
        logger.info(f"Upserted {len(df)} records to {path}")
    except Exception as e:
        # Fallback: không chỉ định partition, để Delta Lake tự detect
        logger.warning(f"Merge failed: {e}. Overwriting table...")
        write_deltalake(path, df, mode='overwrite', storage_options=STORAGE_OPTIONS)
        logger.info(f"Created new Delta table at {path} with {len(df)} records")

def process_ohlc(symbol: str, object_name: str, asset_class: str, unit: str, yf_symbol: str = None):
    """Process and ingest OHLC data from Bronze to Silver
    If Bronze file doesn't exist, fall back to yfinance entirely"""
    logger.info("=" * 60)
    logger.info(f"Processing OHLC for {symbol}")
    logger.info("=" * 60)
    
    try:
        # Try to read from Bronze
        df_clean = pd.DataFrame()
        try:
            logger.info(f"Reading {object_name} from Bronze...")
            obj = minio_client.get_object(MINIO_BUCKET_BRONZE, object_name)
            df_raw = pd.read_csv(io.BytesIO(obj.read()))
            logger.info(f"Read {len(df_raw)} rows from Bronze")
            
            # Clean data
            df_clean = clean_ohlc(df_raw, CSV_COLUMN_MAP_OHLC)
        except Exception as bronze_error:
            logger.warning(f"Bronze file not found ({object_name}): {type(bronze_error).__name__}")
            if yf_symbol:
                logger.info(f"Falling back to yfinance for {symbol}...")
            else:
                raise ValueError(f"Bronze file missing and no yfinance symbol provided for {symbol}")
        
        # Get missing dates from yfinance if symbol provided
        df_yf = pd.DataFrame()
        if yf_symbol:
            if df_clean.empty:
                # No Bronze data, fetch all from yfinance
                logger.info(f"Fetching all historical data from yfinance for {symbol} ({yf_symbol})...")
                try:
                    hist = yf.Ticker(yf_symbol).history(period='max', interval='1d')
                    if not hist.empty:
                        df = hist.reset_index()
                        if df['Date'].dt.tz is not None:
                            df['Date'] = df['Date'].dt.tz_localize(None)
                        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
                        df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                        df['change_percent'] = None
                        df_clean = df[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]
                        logger.info(f"Fetched {len(df_clean)} rows from yfinance")
                except Exception as yf_error:
                    logger.error(f"yfinance error for {symbol}: {yf_error}")
                    raise
            else:
                # Have Bronze data, fill missing dates from yfinance
                raw_dates = set(df_clean['date'].dropna().unique())
                df_yf = get_yfinance_missing(symbol, raw_dates, yf_symbol)
        
        # Combine data
        df_full = pd.concat([df_clean, df_yf], ignore_index=True) if not df_yf.empty else df_clean
        print(f"df_full nulls:\n{df_full.isnull().sum()}")
        print(f"df_full head:\n{df_full.head().to_string()}")
        
        if not df_yf.empty:
            logger.info(f"Missing dates filled: {df_yf['date'].nunique()} | {df_yf['date'].min()} → {df_yf['date'].max()}")
        else:
            logger.info("No missing dates in CSV")
        
        df_full = df_full.sort_values('date').reset_index(drop=True)
        
        # Calculate change_percent if missing
        df_full['change_percent'] = df_full['close'].pct_change() * 100
        df_full.loc[0, 'change_percent'] = 0
        
        # Add metadata columns
        df_full['symbol']      = symbol
        df_full['asset_class'] = asset_class
        df_full['unit']        = unit
        df_full['prev_close']  = None
        df_full['change']      = None
        
        # Determine data source
        data_source = 'yfinance' if df_clean.empty else 'historical_csv'
        df_full['source']      = data_source
        
        # Format output
        ohlc = df_full[['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 'volume', 'change_percent', 'prev_close', 'change', 'source']]
        ohlc = ohlc.sort_values('date').reset_index(drop=True)
        
        # Type conversion
        ohlc['prev_close'] = ohlc['prev_close'].astype('float64')
        ohlc['change'] = ohlc['change'].astype('float64')
        ohlc['symbol'] = ohlc['symbol'].astype(str)
        ohlc['asset_class'] = ohlc['asset_class'].astype(str)
        ohlc['unit'] = ohlc['unit'].astype(str)
        ohlc['source'] = ohlc['source'].astype(str)
        
        print(f"ohlc head:\n{ohlc.head().to_string()}")
        print(f"ohlc tail:\n{ohlc.tail().to_string()}")
        
        logger.info(f"\nOHLC Data Summary:")
        logger.info(f"Total records: {len(ohlc)}")
        logger.info(f"Date range: {ohlc['date'].min()} → {ohlc['date'].max()}")
        logger.info(f"Data source: {data_source}")
        
        # Upsert to Silver
        silver_path = f"s3://{MINIO_BUCKET_SILVER}/{asset_class}/historical/{symbol}/"
        upsert_to_silver(ohlc, silver_path)
        logger.info(f"Done: {len(ohlc)} rows | {ohlc['date'].min()} → {ohlc['date'].max()}")
        write_to_csv(ohlc, asset_class, symbol)
        return True
    except Exception as e:
        logger.error(f"Error processing OHLC for {symbol}: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        # Load configuration from product_list.yaml
        # Try multiple possible paths for flexibility (Docker vs local)
        possible_paths = [
            os.path.join(os.path.dirname(__file__), '../ingestion/api_loaders/yaml/product_list.yaml'),
            '/app/ingestion/api_loaders/yaml/product_list.yaml',  # Docker path
            'ingestion/api_loaders/yaml/product_list.yaml',  # Relative from workspace
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
        logger.info(f"✅ Loaded configuration successfully")
        
        # Process currencies from YAML
        if 'currencies' in config and config['currencies']:
            logger.info(f"Processing {len(config['currencies'])} currencies...")
            for currency in config['currencies']:
                symbol = currency.get('symbol')
                yf_ticker = currency.get('yf_ticker')
                bronze_path = currency.get('bronze_path')
                
                if not symbol or not yf_ticker or not bronze_path:
                    logger.warning(f"Missing symbol/yf_ticker/bronze_path for currency: {currency}")
                    continue
                
                process_ohlc(
                    symbol=symbol,
                    object_name=bronze_path,
                    asset_class='currency',
                    unit='VND',
                    yf_symbol=yf_ticker
                )
        
        # Process indices from YAML
        if 'indices' in config and config['indices']:
            logger.info(f"Processing {len(config['indices'])} indices...")
            for index in config['indices']:
                symbol = index.get('symbol')
                yf_ticker = index.get('yf_ticker')
                bronze_path = index.get('bronze_path')
                
                if not symbol or not yf_ticker or not bronze_path:
                    logger.warning(f"Missing symbol/yf_ticker/bronze_path for index: {index}")
                    continue
                
                # Determine unit based on exchange
                exchange = index.get('exchange', 'US')
                unit_map = {'US': 'USD', 'JP': 'JPY', 'HK': 'HKD', 'DE': 'EUR'}
                unit = unit_map.get(exchange, 'USD')
                
                process_ohlc(
                    symbol=symbol,
                    object_name=bronze_path,
                    asset_class='index',
                    unit=unit,
                    yf_symbol=yf_ticker
                )
        
        # Process commodities from YAML (products section)
        if 'products' in config and config['products']:
            logger.info(f"Processing {len(config['products'])} commodities...")
            for product_dict in config['products']:
                for sym, product_info in product_dict.items():
                    yf_symbol = product_info.get('symbol')
                    name = product_info.get('name', sym)
                    unit = product_info.get('unit', 'USD')
                    source = product_info.get('source')
                    bronze_path = product_info.get('bronze_path')
                    
                    if not yf_symbol or not bronze_path or source != 'yfinance':
                        # Only process yfinance products with bronze_path
                        continue
                    
                    # Use yf_symbol as yfinance ticker (e.g., 'BZ=F' for Brent)
                    process_ohlc(
                        symbol=sym,
                        object_name=bronze_path,
                        asset_class='commodity',
                        unit=unit,
                        yf_symbol=yf_symbol
                    )
        
        logger.info("All OHLC processing completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)

