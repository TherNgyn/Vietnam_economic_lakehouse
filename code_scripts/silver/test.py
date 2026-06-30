import os
import pandas as pd
from deltalake import DeltaTable

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}

def test_read_deltalake():
    from datetime import datetime
    processing_date = datetime.utcnow().strftime("%Y-%m-%d")
    out_path = f"s3://{SILVER_BUCKET}/industry_product"
    
    try:
        dt = DeltaTable(out_path, storage_options=DELTA_STORAGE_OPTIONS)
        df = dt.to_pandas()
        
        print("=== KẾT QUẢ ĐỌC THỬ DELTA LAKE ===")
        print(f"Đường dẫn: {out_path}")
        print(f"Số lượng dòng: {len(df)}")
        print("\nCấu trúc các cột:")
        print(df.dtypes)
        print("\nDữ liệu mẫu (5 dòng đầu):")
        print(df)
        # print("\nDữ liệu mẫu (5 dòng cuối):")
        # print(df.tail())
        # in ra năm nhỏ nhất
        if 'year' in df.columns:
            min_year = df['year'].min()
            print(f"\nNăm nhỏ nhất trong cột 'year': {min_year}")
        else:
            print("\nCột 'year' không tồn tại trong DataFrame.")
        # in ra năm lớn nhất
        if 'year' in df.columns:
            max_year = df['year'].max()
            print(f"Năm lớn nhất trong cột 'year': {max_year}")
        else:
            print("\nCột 'year' không tồn tại trong DataFrame.")
    except Exception as e:
        print(f"Lỗi khi đọc Delta Table: {str(e)}")

if __name__ == "__main__":
    test_read_deltalake()