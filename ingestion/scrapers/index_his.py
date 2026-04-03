import yfinance as yf
import pandas as pd
import time

# Cấu hình bao gồm cả Hàng hóa và Chỉ số (Indices)
config = {
    "products": {
        "brent": "BZ=F",
        "wti": "CL=F",
        "gold": "GC=F"
    },
    "indices": {
        "sp500": "^GSPC",
        "nasdaq": "^IXIC",
        "dji": "^DJI"
    }
}

def fetch_all_history(name, symbol, category):
    print(f"--- Đang tải {category}: {name} ({symbol}) ---")
    ticker = yf.Ticker(symbol)
    
    # Lấy toàn bộ lịch sử
    df = ticker.history(period="max")
    
    if df.empty:
        print(f"X Không có dữ liệu cho {symbol}")
        return None
    
    df.reset_index(inplace=True)
    
    # Chuẩn hóa cột ngày (loại bỏ timezone để dễ lưu trữ)
    if df['Date'].dt.tz is not None:
        df['Date'] = df['Date'].dt.tz_localize(None)
    
    df['Product_Key'] = name
    df['Symbol'] = symbol
    df['Category'] = category # Thêm cột phân loại (Index/Commodity)
    
    print(f"V Đã tải {len(df)} dòng.")
    return df

all_data = []

# Duyệt qua từng nhóm trong cấu hình
for category, items in config.items():
    for name, symbol in items.items():
        df_item = fetch_all_history(name, symbol, category)
        if df_item is not None:
            all_data.append(df_item)
        time.sleep(1.5) # Tăng thời gian nghỉ để an toàn hơn

# Gộp và lưu dữ liệu
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("historical_master_data.csv", index=False)
    print("\n--- HOÀN THÀNH: Đã lưu toàn bộ dữ liệu vào file CSV ---")