import io
import os
import pandas as pd
import yfinance as yf
from minio import Minio
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

load_dotenv()

SYMBOL      = 'USDVND'
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
OBJECT_NAME = f"currency/historical/{SYMBOL}.csv"

storage_options = {
    "AWS_ENDPOINT_URL": "http://minio:9000",  
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin123",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}
minio_client = Minio(
    os.getenv("MINIO_HOST", "localhost:9000"),
    access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    secure=False,
)


CSV_COLUMN_MAP = {
    'date':           'Ngày',
    'open':           'Mở',
    'high':           'Cao',
    'low':            'Thấp',
    'close':          'Lần cuối',
    'volume':         'KL',
    'change_percent': '% Thay đổi',
}


def _read_csv_from_minio() -> pd.DataFrame:
    obj = minio_client.get_object(MINIO_BUCKET_BRONZE, OBJECT_NAME)
    return pd.read_csv(io.BytesIO(obj.read()))


def _parse_date(date_str):
    from datetime import datetime
    try:
        return datetime.strptime(str(date_str).strip(), '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None


def _parse_price(val):
    try:
        return float(str(val).strip().replace(',', ''))
    except:
        return None


def _convert_volume(val):
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


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out['date']           = df[CSV_COLUMN_MAP['date']].apply(_parse_date)
    out['close']          = df[CSV_COLUMN_MAP['close']].apply(_parse_price)
    out['open']           = df[CSV_COLUMN_MAP['open']].apply(_parse_price)
    out['high']           = df[CSV_COLUMN_MAP['high']].apply(_parse_price)
    out['low']            = df[CSV_COLUMN_MAP['low']].apply(_parse_price)
    out['volume']         = df[CSV_COLUMN_MAP['volume']].apply(_convert_volume)
    out['change_percent'] = df[CSV_COLUMN_MAP['change_percent']].apply(
        lambda v: float(str(v).strip().replace('%', '')) if str(v).strip().replace('%', '') not in ('nan', '') else None
    )
    return out[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]


def _get_yf_missing(raw_dates: set) -> pd.DataFrame:
    try:
        hist = yf.Ticker('VND=X').history(period='max', interval='1d')
        if hist.empty:
            return pd.DataFrame()
        df = hist.reset_index()
        if df['Date'].dt.tz is not None:
            df['Date'] = df['Date'].dt.tz_localize(None)
        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
        df['change_percent'] = None
        df = df[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]
        missing = sorted(set(df['date'].unique()) - raw_dates)
        print(f"yfinance missing dates: {len(missing)}")
        return df[df['date'].isin(missing)].copy()
    except Exception as e:
        print(f"yfinance error: {e}")
        return pd.DataFrame()


def _upsert_silver(df: pd.DataFrame):
    path = f"s3://{MINIO_BUCKET_SILVER}/currency/historical/{SYMBOL}"
    try:
        dt = DeltaTable(path)
        dt.merge(
            source=df,
            predicate="s.date = t.date AND s.symbol = t.symbol",
            source_alias='s',
            target_alias='t',
        ).when_matched_update_all().when_not_matched_insert_all().execute()
    except Exception:
        write_deltalake(path, df, mode='overwrite', partition_by=['symbol'], storage_options=storage_options)


if __name__ == "__main__":
    df_raw = _read_csv_from_minio()
    print(f"bronze rows: {len(df_raw)}")
    print("df_raw nulls:\n", df_raw.isnull().sum())
    print("df_raw head:\n", df_raw.head().to_string())

    df_clean = _clean(df_raw)
    print("df_clean nulls:\n", df_clean.isnull().sum())
    print("df_clean head:\n", df_clean.head().to_string())

    raw_dates = set(df_clean['date'].dropna().unique())

    df_yf = _get_yf_missing(raw_dates)
    print("df_yf nulls:\n", df_yf.isnull().sum())
    print("df_yf head:\n", df_yf.head().to_string())

    if not df_yf.empty:
        print(f"Số ngày miss trong csv: {df_yf['date'].nunique()} | {df_yf['date'].min()} → {df_yf['date'].max()}")
    else:
        print("Không có ngày miss trong csv.")

    df_full = pd.concat([df_clean, df_yf], ignore_index=True) if not df_yf.empty else df_clean
    print("df_full nulls:\n", df_full.isnull().sum())
    print("df_full head:\n", df_full.head().to_string())

    df_full = df_full.sort_values('date').reset_index(drop=True)
    df_full['change_percent'] = df_full['close'].pct_change() * 100
    df_full.loc[0, 'change_percent'] = 0

    df_full['symbol']      = SYMBOL
    df_full['asset_class'] = 'currency'
    df_full['unit']        = 'VND'
    df_full['prev_close']  = None
    df_full['change']      = None
    df_full['source']      = 'historical_csv'

    

    ohlc = df_full[['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 'volume', 'change_percent', 'prev_close', 'change', 'source']]
    ohlc = ohlc.sort_values('date').reset_index(drop=True)

    ohlc['prev_close'] = ohlc['prev_close'].astype('float64')
    ohlc['change'] = ohlc['change'].astype('float64')
    ohlc['symbol'] = ohlc['symbol'].astype(str)
    ohlc['asset_class'] = ohlc['asset_class'].astype(str)
    ohlc['unit'] = ohlc['unit'].astype(str)
    ohlc['source'] = ohlc['source'].astype(str)

    print("ohlc head:\n", ohlc.head().to_string())
    print("ohlc tail:\n", ohlc.tail().to_string())

    _upsert_silver(ohlc)
    print(f"silver done: {len(ohlc)} rows | {ohlc['date'].min()} → {ohlc['date'].max()}")