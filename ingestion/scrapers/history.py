import yfinance as yf
import pandas as pd
import time

# Danh sách cấu hình từ YAML của bạn
products = {
    "brent": "BZ=F",
    "wti": "CL=F",
    "gasoline": "RB=F",
    "natural_gas": "NG=F",
    "gold": "GC=F",
    "silver": "SI=F"
}

def fetch_all_history(name, symbol):
    print(f"--- Đang tải dữ liệu cho {name} ({symbol}) ---")
    ticker = yf.Ticker(symbol)
    
    # Lấy toàn bộ dữ liệu từ ngày đầu tiên niêm yết
    df = ticker.history(period="max")
    
    if df.empty:
        print(f"X Không có dữ liệu cho {symbol}")
        return None
    
    # Reset index để đưa Date từ index thành một cột dữ liệu
    df.reset_index(inplace=True)
    
    # Thêm thông tin định danh
    df['Product_Key'] = name
    df['Symbol'] = symbol
    
    print(f"V Đã tải {len(df)} dòng dữ liệu.")
    return df

# Chạy vòng lặp lấy dữ liệu
all_dataframes = []
for name, symbol in products.items():
    data = fetch_all_history(name, symbol)
    if data is not None:
        all_dataframes.append(data)
    time.sleep(1) # Nghỉ 1s để tránh bị Yahoo chặn (Rate limit)

# Gộp tất cả thành một bảng lớn (Master Data)
final_df = pd.concat(all_dataframes, ignore_index=True)

# Lưu ra file CSV
final_df.to_csv("historical_data.csv", index=False)