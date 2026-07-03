import os
import pandas as pd
from deltalake import DeltaTable

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

def test_read_deltalake():
    out_path = f"s3a://{SILVER_BUCKET}/m2"
    
    try:
        dt = DeltaTable(out_path, storage_options=DELTA_STORAGE_OPTIONS)
        df = dt.to_pandas()
        
        print("=== KẾT QUẢ ĐỌC DELTA LAKE ===")
        print(f"Đường dẫn: {out_path}")
        print(f"Số lượng dòng: {len(df)}")

        print("\nCấu trúc các cột:")
        print(df.dtypes)

        print("\nDữ liệu mẫu:")
        print(df.head())
        # hiển thị distinct values của các cột
        print("\n=== DISTINCT VALUES CỦA CÁC CỘT ===")
        for col in df.columns:
            distinct_values = df[col].dropna().unique()
            print(f"Cột '{col}': {len(distinct_values)} giá trị khác nhau")
            if len(distinct_values) <= 10:
                print(f"Giá trị khác nhau: {distinct_values}")
            else:
                print(f"Ví dụ giá trị khác nhau: {distinct_values[:10]} ...")
        print(df.i)

        print("\n=== KIỂM TRA DUPLICATE ===")

        # Key đúng cho silver.gdp
        duplicate_keys = ["year", "quarter", "investment_name", "unit"]

        df["is_duplicate"] = df.duplicated(
            subset=duplicate_keys,
            keep=False
        )

        duplicate_df = df[df["is_duplicate"] == True].copy()

        print(f"Số dòng duplicate: {len(duplicate_df)}")

        if len(duplicate_df) > 0:
            print("\nCác dòng duplicate:")
            print(
                duplicate_df
                .sort_values(duplicate_keys + ["ingest_at"])
                .to_string(index=False)
            )

            print("\n=== TÓM TẮT NHÓM DUPLICATE ===")

            duplicate_summary = (
                df.groupby(duplicate_keys, dropna=False)
                .agg(
                    cnt=("value", "size"),
                    distinct_value_cnt=("value", "nunique"),
                    min_value=("value", "min"),
                    max_value=("value", "max"),
                    min_ingest_at=("ingest_at", "min"),
                    max_ingest_at=("ingest_at", "max"),
                )
                .reset_index()
            )

            duplicate_summary = (
                duplicate_summary[duplicate_summary["cnt"] > 1]
                .sort_values("cnt", ascending=False)
            )

            print(duplicate_summary.to_string(index=False))
        else:
            print("Không có duplicate.")

    except Exception as e:
        print(f"Lỗi khi đọc Delta Table: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_read_deltalake()