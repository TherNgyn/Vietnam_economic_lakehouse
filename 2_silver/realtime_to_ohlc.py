"""
Silver Layer: Realtime OHLC Data Processing
Process daily aggregates from Bronze realtime data to Silver

Data Flow:
1. Read realtime intraday ticks from Bronze
   - Path: s3://bronze/raw/{asset_class}/processing_date={YYYY-MM-DD}/
2. Group by symbol and calculate daily OHLC
   - Open: First tick's open price
   - High: Maximum dayHigh across all ticks
   - Low: Minimum dayLow across all ticks
   - Close: Last tick's lastPrice
   - Volume: Sum of all volumes
3. Clean and standardize data
4. Merge into historical Silver tables (upsert logic by date+symbol)
5. Output: s3://silver/{asset_class}/historical/{symbol}/

Configuration:
- Reads asset classes and settings from product_list.yaml
- Supports: currencies, indices, products, vietnam_indices
- Uses YAML settings for asset_class categorization
"""

import os
import io
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

def find_available_dates(asset_class: str, max_lookback_days: int = 3) -> list:
    """
    Find available dates for an asset class in MinIO Bronze
    
    Searches backward from today for available dated folders
    
    Args:
        asset_class: Asset classification
        max_lookback_days: How many days back to search
    
    Returns:
        List of available dates in YYYY-MM-DD format, sorted newest first
    """
    available_dates = []
    
    for days_back in range(max_lookback_days):
        check_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        object_path = f"raw/{asset_class}/processing_date={check_date}"
        
        try:
            objects = list(minio_client.list_objects(MINIO_BUCKET_BRONZE, prefix=object_path, recursive=True))
            if objects:
                available_dates.append(check_date)
        except:
            continue
    
    return available_dates

# ============================================================================
# DATA READING FROM MINIO
# ============================================================================

def load_configuration() -> dict:
    """Load asset configuration from product_list.yaml"""
    possible_paths = [
        os.path.join(os.path.dirname(__file__), '../ingestion/api_loaders/yaml/product_list.yaml'),
        '/app/ingestion/api_loaders/yaml/product_list.yaml',  # Docker path
        'ingestion/api_loaders/yaml/product_list.yaml',  # Relative from workspace
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"Loading configuration from {path}")
            with open(path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Configuration loaded successfully\n")
            return config
    
    logger.error(f"YAML config not found at any of: {possible_paths}")
    raise FileNotFoundError("product_list.yaml not found")

# ============================================================================
# DATA READING FROM MINIO
# ============================================================================

def read_realtime_data(asset_class: str, target_date: str) -> pd.DataFrame:
    """
    Read realtime data from MinIO Bronze for specific date and asset class
    
    Path structure: s3://bronze/raw/{asset_class}/processing_date={YYYY-MM-DD}/
    Supports both Parquet and CSV formats
    
    Args:
        asset_class: Asset classification (stock, index, currency, commodity, vietnam_index)
        target_date: Date in YYYY-MM-DD format
    
    Returns:
        DataFrame with realtime tick data or empty DataFrame if not found
    """
    logger.info(f"Reading realtime data from MinIO for {asset_class} on {target_date}...")
    
    try:
        # Path: raw/{asset_class}/processing_date={target_date}
        object_path = f"raw/{asset_class}/processing_date={target_date}"
        
        # List objects in the directory from MinIO
        objects = minio_client.list_objects(MINIO_BUCKET_BRONZE, prefix=object_path, recursive=True)
        
        all_dfs = []
        file_count = 0
        for obj in objects:
            if not (obj.object_name.endswith('.parquet') or obj.object_name.endswith('.csv')):
                continue
            
            try:
                logger.info(f"  Reading from MinIO: {obj.object_name}...")
                file_obj = minio_client.get_object(MINIO_BUCKET_BRONZE, obj.object_name)
                
                if obj.object_name.endswith('.parquet'):
                    df = pd.read_parquet(io.BytesIO(file_obj.read()))
                else:
                    df = pd.read_csv(io.BytesIO(file_obj.read()))
                
                all_dfs.append(df)
                file_count += 1
                logger.info(f"    ✓ Read {len(df)} records")
            
            except Exception as e:
                logger.warning(f"    ✗ Error reading {obj.object_name}: {e}")
                continue
        
        if not all_dfs:
            logger.warning(f"No realtime data found in MinIO for {asset_class} on {target_date}")
            logger.info(f"   (looked in: s3://{MINIO_BUCKET_BRONZE}/{object_path})")
            return pd.DataFrame()
        
        # Combine all daily data from all files
        df_combined = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"✓ Loaded {len(df_combined)} total records from {file_count} files\n")
        
        return df_combined
    
    except Exception as e:
        logger.error(f"Error reading realtime data from MinIO: {e}")
        return pd.DataFrame()


# ============================================================================
# OHLC CALCULATION FROM INTRADAY TICKS
# ============================================================================

def calculate_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily OHLC from intraday realtime ticks
    
    Aggregation logic:
    - Open: First tick's open price
    - High: Maximum dayHigh across all ticks
    - Low: Minimum dayLow across all ticks
    - Close: Last tick's lastPrice
    - Volume: Sum of all volumes
    - Change: (Close - Open)
    - Change%: (Change / Open) * 100
    
    Args:
        df: DataFrame with realtime tick data (columns: timestamp, symbol, open, dayHigh, dayLow, lastPrice, volume, etc.)
    
    Returns:
        DataFrame with daily OHLC records (one row per symbol)
    """
    logger.info(f"Calculating daily OHLC from {len(df)} intraday ticks...")
    
    ohlc_rows = []
    
    for symbol, grp in df.groupby('symbol'):
        try:
            # Sort by timestamp to get proper open/close order
            grp = grp.sort_values('timestamp')
            
            # Extract date from first record's timestamp
            date_str = grp['timestamp'].iloc[0][:10] if isinstance(grp['timestamp'].iloc[0], str) else str(grp['timestamp'].iloc[0])[:10]
            
            # Calculate OHLC from intraday ticks
            open_price = float(grp.iloc[0]['open']) if 'open' in grp.columns else float(grp.iloc[0].get('lastPrice', 0))
            close_price = float(grp.iloc[-1]['lastPrice'])
            high_price = float(grp['dayHigh'].max()) if 'dayHigh' in grp.columns else float(grp['lastPrice'].max())
            low_price = float(grp['dayLow'].min()) if 'dayLow' in grp.columns else float(grp['lastPrice'].min())
            volume = float(grp['volume'].sum()) if 'volume' in grp.columns else 0.0
            prev_close = float(grp.iloc[0].get('previousClose', None)) if 'previousClose' in grp.columns else None
            
            # Calculate daily change statistics
            change = close_price - open_price if open_price else 0
            change_pct = (change / open_price * 100) if open_price else 0
            
            # Get asset class and unit from first record
            asset_class = grp.iloc[0].get('asset_class', 'unknown')
            unit = grp.iloc[0].get('unit', '')
            
            ohlc_rows.append({
                'date': date_str,
                'symbol': symbol,
                'asset_class': asset_class,
                'unit': unit,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'change_percent': change_pct,
                'prev_close': prev_close,
                'change': change,
                'source': 'realtime'
            })
            
            logger.info(f"  {symbol}: O={open_price:.4f} H={high_price:.4f} L={low_price:.4f} C={close_price:.4f} V={volume:.0f}")
        
        except Exception as e:
            logger.warning(f"Error calculating OHLC for {symbol}: {e}")
            continue
    
    result_df = pd.DataFrame(ohlc_rows)
    logger.info(f"✓ Calculated OHLC for {len(result_df)} symbols\n")
    
    return result_df


# ============================================================================
# DATA CLEANING AND VALIDATION
# ============================================================================

def clean_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate OHLC data
    
    Operations:
    1. Remove rows with missing critical values (date, symbol, close)
    2. Ensure numeric columns are properly typed
    3. Type conversion for string columns
    4. Remove invalid or null values
    
    Args:
        df: Raw OHLC DataFrame
    
    Returns:
        Cleaned OHLC DataFrame
    """
    logger.info(f"Cleaning {len(df)} OHLC records...")
    
    if df.empty:
        logger.warning("Cannot clean empty DataFrame")
        return df
    
    # Remove rows with critical missing values
    df = df.dropna(subset=['date', 'symbol', 'close'])
    
    # Ensure numeric columns
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'change_percent', 'prev_close', 'change']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Type conversion for string columns
    df['symbol'] = df['symbol'].astype(str)
    df['asset_class'] = df['asset_class'].astype(str)
    df['unit'] = df['unit'].astype(str)
    df['source'] = df['source'].astype(str)
    
    logger.info(f"✓ Cleaned to {len(df)} valid records\n")
    
    return df

# ============================================================================
# SILVER UPSERT TO MINIO
# ============================================================================

def upsert_to_silver(df: pd.DataFrame, asset_class: str):
    """
    Upsert daily OHLC data to Silver using Delta Lake merge
    
    Merge Strategy:
    - Predicate: (date, symbol) combination
    - When matched: Update all columns
    - When not matched: Insert all columns
    - If table doesn't exist: Create new with symbol partitioning
    
    Output path: s3://silver/{asset_class}/historical/{symbol}/
    
    Args:
        df: Cleaned OHLC DataFrame
        asset_class: Asset classification for path structure
    """
    logger.info(f"Upserting {len(df)} records to MinIO Silver by symbol...\n")
    
    if df.empty:
        logger.warning("No data to upsert")
        return
    
    successful = 0
    failed = 0
    
    for symbol, grp in df.groupby('symbol'):
        try:
            logger.info(f"  [{symbol}] Processing {len(grp)} records...")
            
            # Select required columns in consistent order
            ohlc = grp[['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 
                        'volume', 'change_percent', 'prev_close', 'change', 'source']]
            
            # Target path in MinIO Silver
            silver_path = f"s3://{MINIO_BUCKET_SILVER}/{asset_class}/historical/{symbol}"
            
            try:
                # Try to merge into existing table
                dt = DeltaTable(silver_path, storage_options=STORAGE_OPTIONS)
                dt.merge(
                    source=ohlc,
                    predicate="s.date = t.date AND s.symbol = t.symbol",
                    source_alias='s',
                    target_alias='t',
                ).when_matched_update_all().when_not_matched_insert_all().execute()
                
                logger.info(f"    ✓ Upserted {len(ohlc)} records to existing table")
                successful += 1
            
            except Exception as merge_error:
                # If table doesn't exist, create new
                logger.info(f"    Creating new table: {type(merge_error).__name__}")
                write_deltalake(
                    silver_path, 
                    ohlc, 
                    mode='overwrite', 
                    partition_by=['symbol'],
                    storage_options=STORAGE_OPTIONS
                )
                logger.info(f"    ✓ Created new table with {len(ohlc)} records")
                successful += 1
        
        except Exception as e:
            logger.error(f"    ✗ Error upserting {symbol}: {e}")
            failed += 1
            continue
    
    logger.info(f"\n✓ Upsert complete: {successful} successful, {failed} failed\n")


# ============================================================================
# MAIN PROCESSING PIPELINE
# ============================================================================

def process_daily_realtime(asset_class: str, target_date: str = None):
    """
    Main processing pipeline for realtime OHLC
    
    Data Flow:
    1. Read realtime data from MinIO Bronze
    2. Calculate OHLC from intraday ticks
    3. Clean and validate
    4. Upsert to MinIO Silver
    
    Args:
        asset_class: Asset classification (stock, index, currency, commodity, vietnam_index)
        target_date: Date in YYYY-MM-DD format (default: yesterday)
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 80)
    logger.info(f"Processing Daily Realtime OHLC: {asset_class}")
    logger.info("=" * 80)
    logger.info("")
    
    if target_date is None:
        # Try to find available dates (today first, then yesterday, then earlier)
        available_dates = find_available_dates(asset_class)
        if available_dates:
            target_date = available_dates[0]
            logger.info(f"Auto-detected date: {target_date} (available dates: {', '.join(available_dates)})")
        else:
            # Fall back to yesterday if nothing found yet
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            logger.info(f"No data found in recent dates, trying: {target_date}")
    
    logger.info(f"Target date: {target_date}")
    logger.info("")
    
    try:
        # Step 1: Read realtime data from MinIO Bronze
        df_raw = read_realtime_data(asset_class, target_date)
        
        if df_raw.empty:
            logger.warning(f"No realtime data found for {asset_class} on {target_date}")
            return False
        
        # Step 2: Calculate OHLC from intraday ticks
        df_ohlc = calculate_ohlc(df_raw)
        
        if df_ohlc.empty:
            logger.warning("Failed to calculate OHLC")
            return False
        
        # Step 3: Clean data
        df_clean = clean_ohlc(df_ohlc)
        
        if df_clean.empty:
            logger.warning("No valid records after cleaning")
            return False
        
        # Step 4: Upsert to MinIO Silver
        upsert_to_silver(df_clean, asset_class)
        
        # Summary
        logger.info("=" * 80)
        logger.info(f"✅ PROCESS COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Asset Class  : {asset_class}")
        logger.info(f"Date         : {target_date}")
        logger.info(f"Ticks Loaded : {len(df_raw)}")
        logger.info(f"OHLC Records : {len(df_clean)}")
        logger.info(f"Symbols      : {df_clean['symbol'].nunique()}")
        logger.info("=" * 80)
        logger.info("")
        
        return True
    
    except Exception as e:
        logger.error(f"Fatal error processing {asset_class}: {e}", exc_info=True)
        return False

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import sys
    
    try:
        # Load configuration
        config = load_configuration()
        logger.info("")
        
        # Determine asset classes to process from configuration
        asset_classes = []
        
        if config.get('currencies'):
            asset_classes.append('currency')
        if config.get('indices'):
            asset_classes.append('index')
        if config.get('products'):
            asset_classes.append('commodity')
        if config.get('vietnam_indices'):
            asset_classes.append('vietnam_index')
        
        # Add custom asset class if data supports it
        if config.get('stocks'):
            asset_classes.append('stock')
        
        # Get target date from command line or environment
        target_date = None
        if len(sys.argv) > 1:
            target_date = sys.argv[1]
            logger.info(f"Using date from command line: {target_date}")
        else:
            target_date_env = os.getenv('REALTIME_DATE')
            if target_date_env:
                target_date = target_date_env
                logger.info(f"Using date from environment (REALTIME_DATE): {target_date}")
        
        if target_date:
            logger.info(f"Processing {len(asset_classes)} asset classes for specified date: {target_date}\n")
        else:
            logger.info(f"Processing {len(asset_classes)} asset classes (auto-detecting most recent date)\n")
            logger.info("Tip: Specify date with: python realtime_to_ohlc.py YYYY-MM-DD")
            logger.info("     Or set REALTIME_DATE environment variable\n")
        
        logger.info(f"Asset classes: {', '.join(asset_classes)}\n")
        
        # Process each asset class
        results = {}
        for asset_class in asset_classes:
            results[asset_class] = process_daily_realtime(asset_class, target_date)
        
        # Final summary
        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        for asset_class, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            logger.info(f"{asset_class:20} {status}")
        logger.info("=" * 80)
        
        # Show available dates if all failed
        if not any(results.values()):
            logger.warning("")
            logger.warning("All asset classes failed to find data. Checking available dates...")
            for asset_class in asset_classes[:1]:  # Check just one to show available dates
                available = find_available_dates(asset_class, max_lookback_days=7)
                if available:
                    logger.info(f"Available dates for {asset_class}: {', '.join(available)}")
                    logger.info(f"Try: python realtime_to_ohlc.py {available[0]}")
                else:
                    logger.warning(f"No data found for {asset_class} in the last 7 days")
        
        # Exit with error if any processing failed
        if not all(results.values()):
            exit(1)
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)
