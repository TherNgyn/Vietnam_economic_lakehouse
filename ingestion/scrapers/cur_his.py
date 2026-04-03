import yfinance as yf
import pandas as pd
import yaml
import time
from datetime import datetime

# Đọc file cấu hình YAML
with open("./ingestion/scrapers/cur.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

def get_currency_history_yf(symbol: str, period: str = "max"):
    try:
        # Chuẩn hóa mã cho Yahoo Finance
        yf_symbol = f"{symbol}=X"
        if symbol == "USDVND":
            yf_symbol = "VND=X" 
            
        ticker = yf.Ticker(yf_symbol)
        
        # Lấy lịch sử với interval 1 ngày (1d) để có dữ liệu dài nhất
        hist = ticker.history(period=period, interval="1d")
        
        if hist.empty:
            print(f"X Không có dữ liệu lịch sử cho {symbol}")
            return None
        
        # Làm sạch dữ liệu
        df = hist.reset_index()
        
        # Loại bỏ timezone để tương thích với các hệ thống database/excel
        if df['Date'].dt.tz is not None:
            df['Date'] = df['Date'].dt.tz_localize(None)
            
        # Thêm các cột định danh
        df['Symbol'] = symbol
        df['YF_Symbol'] = yf_symbol
        
        # Chỉ giữ lại các cột cần thiết cho lịch sử
        cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol', 'YF_Symbol']
        df = df[cols]
        
        print(f"V Đã tải {len(df)} dòng lịch sử cho {symbol}")
        return df

    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu lịch sử {symbol}: {e}")
        return None

if __name__ == "__main__":
    all_currencies_df = []
    
    for item in PRODUCT_CONFIG.get("currencies", []):
        symbol = item["symbol"]
        print(f"--- Đang lấy lịch sử tỷ giá: {symbol} ---")
        
        df_history = get_currency_history_yf(symbol, period="max")
        
        if df_history is not None:
            all_currencies_df.append(df_history)
        
        # Nghỉ một chút để tránh bị rate limit
        time.sleep(1)

    # Gộp tất cả các cặp tiền vào một DataFrame duy nhất
    if all_currencies_df:
        final_df = pd.concat(all_currencies_df, ignore_index=True)
        
        # Lưu ra CSV để kiểm tra
        filename = f"currency_history_{datetime.now().strftime('%Y%m%d')}.csv"
        final_df.to_csv(filename, index=False)
        print(f"\n--- HOÀN THÀNH: Toàn bộ dữ liệu đã được lưu vào {filename} ---")