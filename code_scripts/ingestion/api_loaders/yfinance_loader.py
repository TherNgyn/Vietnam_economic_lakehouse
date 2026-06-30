import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import yaml
import yfinance as yf

# 1. CONFIG PATHS
CONFIG_PATHS = {
    "currency": "./ingestion/api_loaders/yaml/currency_list.yaml",
    "index": "./ingestion/api_loaders/yaml/index_world.yaml",
    "product": "./ingestion/api_loaders/yaml/product_list.yaml",
}
# Load 
def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def safe_fast_get(fast_info, key: str, default=None):
    try:
        return fast_info.get(key, default)
    except Exception:
        try:
            return fast_info[key]
        except Exception:
            return default


def safe_round(value, decimals: int = 2):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return round(float(value), decimals)
    except Exception:
        return None


def safe_int(value, default: int = 0):
    try:
        if value is None or pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def normalise_datetime_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Datetime" in df.columns and "Date" not in df.columns:
        df = df.rename(columns={"Datetime": "Date"})

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        try:
            if df["Date"].dt.tz is not None:
                df["Date"] = df["Date"].dt.tz_localize(None)
        except Exception:
            pass

    return df


def resolve_yf_symbol(asset: Dict[str, Any]) -> str:
    asset_type = asset["asset_type"]
    symbol = asset["symbol"]

    if asset_type == "currency":
        # Trường hợp đặc biệt USDVND trong Yahoo Finance thường là VND=X
        if symbol == "USDVND":
            return "VND=X"
        return f"{symbol}=X"

    # index và product đã có yf_symbol/yf_ticker trực tiếp
    return asset.get("yf_symbol") or asset.get("yf_ticker") or symbol


def get_decimals(asset: Dict[str, Any]) -> int:
    if asset["asset_type"] == "currency":
        return 2 if "VND" in asset["symbol"] else 4
    return 2


# Chuẩn hóa 
def load_currency_assets() -> List[Dict[str, Any]]:
    config = load_yaml(CONFIG_PATHS["currency"])
    assets = []

    for item in config.get("currencies", []):
        symbol = item["symbol"]

        assets.append({
            "asset_type": "currency",
            "symbol": symbol,
            "name": item.get("name", symbol),
            "category": "currency",
            "currency": item.get("currency", "VND"),
            "unit": item.get("unit", "rate"),
        })

    return assets


def load_index_assets() -> List[Dict[str, Any]]:
    config = load_yaml(CONFIG_PATHS["index"])
    assets = []

    for item in config.get("indices", []):
        symbol = item["symbol"]
        yf_ticker = item["yf_ticker"]

        assets.append({
            "asset_type": "index",
            "symbol": symbol,
            "yf_symbol": yf_ticker,
            "name": item.get("name", symbol),
            "category": "index",
            "currency": item.get("currency", "USD"),
            "unit": item.get("unit", "point"),
        })

    return assets


def load_product_assets() -> List[Dict[str, Any]]:
    config = load_yaml(CONFIG_PATHS["product"])
    assets = []

    for product in config.get("products", []):
        for product_key, product_info in product.items():
            symbol = product_info["symbol"]

            assets.append({
                "asset_type": "product",
                "symbol": symbol,
                "name": product_info.get("name", product_key),
                "product_key": product_key,
                "category": product_info.get("category", "commodity"),
                "currency": product_info.get("currency", "USD"),
                "unit": product_info.get("unit", "unit"),
            })

    return assets


def load_assets(asset_type: str) -> List[Dict[str, Any]]:
    if asset_type == "currency":
        return load_currency_assets()

    if asset_type == "index":
        return load_index_assets()

    if asset_type == "product":
        return load_product_assets()

    if asset_type == "all":
        return (
            load_currency_assets()
            + load_index_assets()
            + load_product_assets()
        )

    raise ValueError(f"Unsupported asset_type: {asset_type}")

# Tải lịch sử 
def fetch_history(asset: Dict[str, Any], period: str = "max") -> Optional[pd.DataFrame]:
    yf_symbol = resolve_yf_symbol(asset)

    try:
        print(f"--- Đang tải lịch sử {asset['asset_type']}: {asset['symbol']} ({yf_symbol}) ---")

        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=period, interval="1d")

        if df.empty:
            print(f"X Không có dữ liệu lịch sử cho {asset['symbol']} ({yf_symbol})")
            return None

        df = df.reset_index()
        df = normalise_datetime_column(df)

        # Chuẩn hoá metadata chung
        df["asset_type"] = asset["asset_type"]
        df["symbol"] = asset["symbol"]
        df["yf_symbol"] = yf_symbol
        df["name"] = asset.get("name", asset["symbol"])
        df["category"] = asset.get("category")
        df["currency"] = asset.get("currency")
        df["unit"] = asset.get("unit")
        df["source"] = "yfinance"
        df["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Giữ các cột phổ biến nếu tồn tại
        base_cols = [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "asset_type",
            "symbol",
            "yf_symbol",
            "name",
            "category",
            "currency",
            "unit",
            "source",
            "ingested_at",
        ]

        existing_cols = [c for c in base_cols if c in df.columns]
        df = df[existing_cols]

        print(f"V Đã tải {len(df)} dòng lịch sử cho {asset['symbol']}")
        return df

    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu lịch sử {asset['symbol']} ({yf_symbol}): {e}")
        return None


def fetch_history_batch(asset_type: str = "all", period: str = "max", sleep_seconds: float = 1.0) -> pd.DataFrame:
    assets = load_assets(asset_type)
    all_dfs = []

    for asset in assets:
        df = fetch_history(asset, period=period)
        if df is not None:
            all_dfs.append(df)

        time.sleep(sleep_seconds)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)

# Tải dữ liệu realtime 
def get_latest_intraday_bar(ticker: yf.Ticker) -> Optional[pd.Series]:
    """
    Ưu tiên dữ liệu 1 phút.
    Nếu không có thì fallback về dữ liệu 1 ngày.
    """
    hist = ticker.history(period="1d", interval="1m")

    if hist.empty:
        hist = ticker.history(period="1d", interval="1d")

    if hist.empty:
        return None

    return hist.iloc[-1]


def fetch_realtime(asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    yf_symbol = resolve_yf_symbol(asset)
    decimals = get_decimals(asset)

    try:
        print(f"--- Đang lấy realtime {asset['asset_type']}: {asset['symbol']} ({yf_symbol}) ---")

        ticker = yf.Ticker(yf_symbol)
        fast_info = ticker.fast_info
        current_bar = get_latest_intraday_bar(ticker)

        if current_bar is None:
            print(f"X Không có dữ liệu realtime cho {asset['symbol']} ({yf_symbol})")
            return None

        last_price = safe_fast_get(fast_info, "last_price", current_bar.get("Close"))
        previous_close = safe_fast_get(fast_info, "previous_close", None)

        # Fallback nếu previous_close thiếu
        if previous_close is None:
            previous_close = current_bar.get("Open")

        change = None
        change_percent = None

        if last_price is not None and previous_close not in [None, 0]:
            change = float(last_price) - float(previous_close)
            change_percent = (change / float(previous_close)) * 100

        currency = safe_fast_get(
            fast_info,
            "currency",
            asset.get("currency", "USD")
        )

        data = {
            "asset_type": asset["asset_type"],
            "symbol": asset["symbol"],
            "yf_symbol": yf_symbol,
            "name": asset.get("name", asset["symbol"]),
            "category": asset.get("category"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

            "price": safe_round(last_price, decimals),
            "open": safe_round(current_bar.get("Open"), decimals),
            "high": safe_round(current_bar.get("High"), decimals),
            "low": safe_round(current_bar.get("Low"), decimals),
            "prev_close": safe_round(previous_close, decimals),
            "volume": safe_int(current_bar.get("Volume", 0)),

            "change": safe_round(change, decimals),
            "change_percent": safe_round(change_percent, 2),

            "currency": currency,
            "unit": asset.get("unit"),
            "data_type": f"{asset['asset_type']}-realtime",
            "source": "yfinance",
        }

        return data

    except Exception as e:
        print(f"Lỗi khi lấy realtime {asset['symbol']} ({yf_symbol}): {e}")
        return None


def fetch_realtime_batch(asset_type: str = "all", sleep_seconds: float = 1.0) -> List[Dict[str, Any]]:
    assets = load_assets(asset_type)
    results = []

    for asset in assets:
        data = fetch_realtime(asset)
        if data:
            results.append(data)

        time.sleep(sleep_seconds)

    return results


# =========================================================
# 6. CLI ENTRYPOINT
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Unified yfinance loader")

    parser.add_argument(
        "--mode",
        choices=["history", "realtime"],
        required=True,
        help="history hoặc realtime"
    )

    parser.add_argument(
        "--asset-type",
        choices=["currency", "index", "product", "all"],
        default="all",
        help="Loại asset cần lấy"
    )

    parser.add_argument(
        "--period",
        default="max",
        help="Period cho history, ví dụ: max, 1y, 5y, 1mo"
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Thời gian nghỉ giữa mỗi request"
    )

    args = parser.parse_args()

    if args.mode == "history":
        final_df = fetch_history_batch(
            asset_type=args.asset_type,
            period=args.period,
            sleep_seconds=args.sleep
        )

        if final_df.empty:
            print("Không có dữ liệu history.")
        else:
            print(final_df.head(10))
            print(f"Tổng số dòng history: {len(final_df)}")

            # Nếu muốn lưu local bronze tạm thời
            output_path = Path(f"./output/yfinance_{args.asset_type}_history.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

            print(f"Đã lưu history vào: {output_path}")

    elif args.mode == "realtime":
        results = fetch_realtime_batch(
            asset_type=args.asset_type,
            sleep_seconds=args.sleep
        )

        if not results:
            print("Không có dữ liệu realtime.")
        else:
            print(json.dumps(results, indent=2, ensure_ascii=False))

            # Nếu muốn lưu JSON local bronze tạm thời
            output_path = Path(f"./output/yfinance_{args.asset_type}_realtime.json")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            print(f"Đã lưu realtime vào: {output_path}")
if __name__ == "__main__":
    main()