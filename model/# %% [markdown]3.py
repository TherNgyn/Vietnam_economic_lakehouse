# %% [markdown]
# # Chuyển Đổi Dữ Liệu Thành Unified Dataset
# ## Chi Tiết Từng File Dữ Liệu & Thống Kê
# 
# Notebook này:
# - **Đọc** 24 file CSV từ `data/raw/`
# - **Kiểm tra** chi tiết từng file (rows, columns, date range)
# - **Tạo** unified dataset với 360 tháng (1995-2024)
# - **Xử lý** missing values (2,633 → 0)
# - **Xuất** chỉ 1 file đầu ra: `cpi_forecast_full_dataset_advanced.csv`

# %% [markdown]
# ## Setup & Chuẩn Bị

# %%
import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
script_dir = os.path.abspath('') 

data_raw_dir = os.path.join(script_dir, 'data', 'raw')
data_processed_dir = os.path.join(script_dir, 'data', 'processed')

os.makedirs(data_raw_dir, exist_ok=True)
os.makedirs(data_processed_dir, exist_ok=True)

print(f"{script_dir}")

# %% [markdown]
# ## BƯỚC 1: Chi Tiết Từng File Dữ Liệu

# %% [markdown]
# Bỏ 2018 + gasoline

# %%
data_config = {
    # Economic
    'cpi_mom_processed.csv': {'date_col': 'date', 'value_cols': ['cpi', 'inflation'], 'category': 'Economic'},
    'core_inflation_rate.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},
    #'cpi_base_year.csv': {'date_col': 'date', 'value_cols': ['base_2010'], 'category': 'Economic'},
    'interest_rate.csv': {'date_col': 'date', 'value_cols': ['interest_rate'], 'category': 'Economic', 'filter_term': '3 Months'},
    'ppi_qoq.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},
    'm2.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},
    'broad_money.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},
    'policy_rate.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},
    'gdp.csv': {'date_col': 'date', 'value_cols': ['value'], 'category': 'Economic'},

    # Commodities
    'brent.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    'wti.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    #'gasoline.csv': {'date_col': 'date', 'value_cols': ['price'], 'category': 'Commodity', 'filter_product': 'RON 95'},
    'gasoline_world.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    'natural_gas.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    'gold.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    'silver.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Commodity'},
    
    # Vietnam stocks
    'VNINDEX.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'VN_Stock'},
    'VN30.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'VN_Stock'},
    'HNX.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'VN_Stock'},
    'UPCOM.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'VN_Stock'},
    
    # Global stocks
    'NASDAQ.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    'S&P500.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    'DAX.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    'DOWJONES.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    'NIKKEI225.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    'HANGSENG.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Global_Stock'},
    
    # Currency
    'USDVND.csv': {'date_col': 'date', 'value_cols': ['close'], 'category': 'Currency'},
}

# %% [markdown]
# ## BƯỚC 2: Tải & Kiểm Tra Chi Tiết

# %%
loaded_data = {}
file_details = []
failed = []

for filename in sorted(os.listdir(data_raw_dir)):
    if not filename.endswith('.csv'):
        continue
    
    try:
        filepath = os.path.join(data_raw_dir, filename)
        df = pd.read_csv(filepath)
        file_size = os.path.getsize(filepath) / 1024
        print(f"FILE: {filename}")
        print(f"Total rows: {len(df)}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        # tỷ lệ null
        print("Null values (%):")
        print((df.isnull().mean() * 100).round(2))
        print(df.head())

        date_col = None
        if 'date' in df.columns:
            date_col = 'date'
        else:
            for col in df.columns:
                try:
                    test = pd.to_datetime(df[col].iloc[:10], errors='coerce')
                    if test.notna().sum() > 5:
                        date_col = col
                        print(f"Auto-detected date column: '{date_col}'")
                        break
                except:
                    continue
        
        if date_col is None:
            failed.append((filename, "No date column detected"))
            continue

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        value_cols = [col for col in df.columns if col != date_col]
        print(f"Value columns to use ({len(value_cols)}): {value_cols}")

    except Exception as e:
        failed.append((filename, str(e)))
        print(f"ERROR: {str(e)[:80]}")

# %%
loaded_data = {}
file_details = []
failed = []
null_analysis = []

for filename, config in data_config.items():
    try:
        filepath = os.path.join(data_raw_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"{filename:25} | NOT FOUND")
            continue

        df = pd.read_csv(filepath)
        # Lấy 3M và RON 95 
        if filename == 'interest_rate.csv' and 'term' in df.columns:
            term_to_use = config.get('filter_term', '3M')
            df = df[df['term'] == term_to_use].copy()
        
        # if filename == 'gasoline.csv' and 'product' in df.columns:
        #     product_to_use = config.get('filter_product', 'RON 95')
        #     df = df[df['product'] == product_to_use].copy()
        
        file_size = os.path.getsize(filepath) / 1024
        date_col = config['date_col']

        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

            value_cols = [col for col in config['value_cols'] if col in df.columns]
            
            if value_cols:
                subset = df[[date_col] + value_cols].copy()
                subset = subset.dropna(subset=[date_col] + value_cols)
                # chỉ xóa các cột có 100% null, còn lại giữ lại để phân tích
                for col in subset.columns:
                    if col != date_col:
                        null_count = subset[col].isnull().sum()
                        null_pct = (null_count / len(subset) * 100)
                        if null_pct > 0:
                            null_analysis.append({
                                'File': filename,
                                'Column': col,
                                'Null_Count': null_count,
                                'Null_Percent': f"{null_pct:.2f}%"
                            })
                subset = subset.dropna(axis=1, how='all')
                
                subset = subset.rename(columns={date_col: 'date'})
                for col in subset.columns:
                    if col != 'date':
                        if len(value_cols) == 1:
                            subset = subset.rename(columns={col: filename.replace('.csv', '')})
                        else:
                            subset = subset.rename(columns={col: f"{filename.replace('.csv', '')}_{col}"})
                
                loaded_data[filename] = subset

                file_info = {
                    'Filename': filename,
                    'Category': config['category'],
                    'Total_Rows': len(df),
                    'Data_Rows': len(subset),
                    'Date_Start': subset['date'].min().date(),
                    'Date_End': subset['date'].max().date(),
                    'Columns': len(subset.columns),
                }
                file_details.append(file_info)
                
                print(f"{filename:25} | {len(subset):6,} rows | {config['category']:15}")
    
    except Exception as e:
        failed.append((filename, str(e)))

# %% [markdown]
# ### Chi Tiết Từng File

# %%
if null_analysis:
    null_df = pd.DataFrame(null_analysis)
    for file in null_df['File'].unique():
        file_nulls = null_df[null_df['File'] == file]
        print(f"{file}:")
        for _, row in file_nulls.iterrows():
            print(f"{row['Column']:30} | {row['Null_Count']:5} nulls ({row['Null_Percent']})")
else:
    print("No null value")

# %%
file_details_df = pd.DataFrame(file_details)

for category in file_details_df['Category'].unique():
    category_files = file_details_df[file_details_df['Category'] == category]
    total_rows = category_files['Data_Rows'].astype(int).sum()
    print(f"  • {category:20} | {len(category_files):2} files | {total_rows:,} total rows")

for idx, row in file_details_df.iterrows():
    print(f"""
  File {idx+1}: {row['Filename']}
    - Category: {row['Category']}
    - Rows: {row['Total_Rows']} (sau lọc: {row['Data_Rows']})
    - Date Range: {row['Date_Start']} to {row['Date_End']}
    - Columns: {row['Columns']}
  """)

# %% [markdown]
# ## BƯỚC 3: Gộp Tần Suất Thành Hàng Tháng

# %%
min_date = pd.Timestamp('1995-01-01')
# max_day của cpi_mom 
max_date  = pd.Timestamp('2024-12-01')
time_axis = pd.date_range(start=min_date, end=max_date, freq='MS')
time_index = pd.DataFrame({'date': time_axis})

print(f"  Standard time axis: {time_axis[0].date()} to {time_axis[-1].date()}")
print(f"  Total months: {len(time_axis)}")

# %%
frequency_analysis = []

for filename, config in data_config.items():
    try:
        filepath = os.path.join(data_raw_dir, filename)
        
        if not os.path.exists(filepath):
            continue

        df = pd.read_csv(filepath)

        if filename == 'interest_rate.csv' and 'term' in df.columns:
            term_to_use = config.get('filter_term', '3M')
            df = df[df['term'] == term_to_use].copy()
        
        if filename == 'gasoline.csv' and 'product' in df.columns:
            product_to_use = config.get('filter_product', 'RON 95')
            df = df[df['product'] == product_to_use].copy()
        
        date_col = config['date_col']
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

            df_sorted = df.sort_values(date_col)
            dates = df_sorted[date_col].unique()
            
            if len(dates) > 1:
                date_diffs = pd.Series(dates[1:]) - pd.Series(dates[:-1])
                min_diff = date_diffs.min().days
                max_diff = date_diffs.max().days
                mode_diff = date_diffs.mode()[0].days if len(date_diffs.mode()) > 0 else None

                if mode_diff == 1:
                    freq_type = "DAILY"
                elif mode_diff == 7:
                    freq_type = "WEEKLY"
                elif 28 <= mode_diff <= 31:
                    freq_type = "MONTHLY"
                elif mode_diff == 365 or mode_diff == 366:
                    freq_type = "ANNUAL"
                else:
                    freq_type = f"OTHER ({mode_diff}d)"
                
                rows_per_day = len(df) / ((df[date_col].max() - df[date_col].min()).days + 1)
                
                frequency_analysis.append({
                    'File': filename,
                    'Category': config['category'],
                    'Total_Rows': len(df),
                    'Date_Range': f"{df_sorted[date_col].min().date()} to {df_sorted[date_col].max().date()}",
                    'Frequency': freq_type,
                    'Min_Gap_Days': min_diff,
                    'Max_Gap_Days': max_diff,
                    'Rows_Per_Day': f"{rows_per_day:.2f}",
                    'Needs_Resample': "NO" if freq_type == "MONTHLY" else "YES"
                })
    
    except Exception as e:
        pass

freq_df = pd.DataFrame(frequency_analysis)

for freq in freq_df['Frequency'].unique():
    files_with_freq = freq_df[freq_df['Frequency'] == freq]
    print(f"\n{freq:10} ({len(files_with_freq)} files):")
    for _, row in files_with_freq.iterrows():
        needs = "RESAMPLE" if row['Needs_Resample'] == "YES" else "KEEP AS-IS"
        print(f"  • {row['File']:25} | {row['Total_Rows']:6,} rows | {needs}")


print(f"{'File':<25} {'Category':<18} {'Frequency':<10} {'Total Rows':<12} {'Resample?':<12}")
print("-" * 120)

for _, row in freq_df.iterrows():
    needs = row['Needs_Resample']
    print(f"{row['File']:<25} {row['Category']:<18} {row['Frequency']:<10} {row['Total_Rows']:<12,} {needs:<12}")

resample_count = (freq_df['Needs_Resample'] == "YES").sum()
keep_count = (freq_df['Needs_Resample'] == "NO").sum()

print("\n" + "="*120)
print(f"KHÔNG RESAMPLE (đã monthly): {keep_count} files")
print(f"CẦN RESAMPLE (daily/other): {resample_count} files")


# %%
monthly_data = {}
agg_report = []

files_keep_monthly = {'cpi_mom_processed.csv', 'core_inflation_rate.csv', 'm2.csv','policy_rate.csv','broad_money.csv'}  # Đã monthly - giữ nguyên
files_downsample = set(freq_df[freq_df['Frequency'] == 'DAILY']['File'].values)  

files_quarterly = {'ppi_qoq.csv', 'gdp.csv'}      # Quarterly - 1 value/quarter → repeat cho 3 tháng
files_irregular = {'interest_rate.csv'} # Irregular - dùng ffill

for filename, df in loaded_data.items():
    try:
        df_sorted = df.sort_values('date').copy()

        if filename in files_keep_monthly:
            monthly_data[filename] = df_sorted
            print(f" {filename:25} | KEEP MONTHLY | {len(df_sorted)} rows")
            agg_report.append({
                'Filename': filename,
                'Strategy': 'KEEP_MONTHLY',
                'Original_Rows': len(df),
                'Monthly_Rows': len(df_sorted),
                'Features': len(df_sorted.columns) - 1
            })

        elif filename in files_downsample:
            df_agg = df_sorted.copy()
            df_agg = df_agg.set_index('date')
            value_cols = [col for col in df_agg.columns]
            
            resampled = df_agg[value_cols].resample('MS').agg({
                col: 'last'
                for col in value_cols
            })
            resampled = resampled.reset_index()
            
            monthly_data[filename] = resampled
            compression_ratio = (len(resampled) / len(df)) * 100
            
            print(f"⬇ {filename:25} | DOWNSAMPLE Daily→Monthly | {len(df):6,} → {len(resampled):4,} rows | Compression {compression_ratio:.1f}%")
            agg_report.append({
                'Filename': filename,
                'Strategy': 'DOWNSAMPLE_DAILY',
                'Original_Rows': len(df),
                'Monthly_Rows': len(resampled),
                'Features': len(resampled.columns) - 1
            })

        elif filename in files_quarterly:
            df_agg = df_sorted.copy()
            df_agg = df_agg.set_index('date')
            value_cols = [col for col in df_agg.columns]
  
            resampled = df_agg[value_cols].resample('MS').ffill()
            resampled = resampled.reset_index()
            
            monthly_data[filename] = resampled
            print(f"⬆ {filename:25} | UPSAMPLE Quarterly→Monthly (repeat 3 months) | {len(df):6,} → {len(resampled):6,} rows | Forward Fill")
            agg_report.append({
                'Filename': filename,
                'Strategy': 'UPSAMPLE_QUARTERLY_FFILL',
                'Original_Rows': len(df),
                'Monthly_Rows': len(resampled),
                'Features': len(resampled.columns) - 1
            })

        elif filename in files_irregular:
            df_agg = df_sorted.copy()
            df_agg = df_agg.set_index('date')
            value_cols = [col for col in df_agg.columns]
            
            resampled = df_agg[value_cols].resample('MS').ffill()
            resampled = resampled.reset_index()
            
            monthly_data[filename] = resampled
            print(f"⬆ {filename:25} | UPSAMPLE Irregular→Monthly | {len(df):6,} → {len(resampled):6,} rows | Forward Fill")
            agg_report.append({
                'Filename': filename,
                'Strategy': 'UPSAMPLE_IRREGULAR_FFILL',
                'Original_Rows': len(df),
                'Monthly_Rows': len(resampled),
                'Features': len(resampled.columns) - 1
            })
    
    except Exception as e:
        print(f"✗ {filename:25} | ERROR: {str(e)[:40]}")

print(f"\n Resample thành công: {len(monthly_data)} files")

agg_df = pd.DataFrame(agg_report)
for strategy in agg_df['Strategy'].unique():
    strategy_df = agg_df[agg_df['Strategy'] == strategy]
    total_features = strategy_df['Features'].sum()
    print(f"{strategy:30} | {len(strategy_df):2d} files | Total rows: {strategy_df['Original_Rows'].sum():,} → {strategy_df['Monthly_Rows'].sum():,} | Total features: {total_features:3d}")


# %%
first_file = list(monthly_data.keys())[2]
sample_df = monthly_data[first_file]

print(f"\n📋 SAMPLE RESAMPLED FILE: {first_file}")
print(f"Shape: {sample_df.shape} (rows × columns)")
print(f"\nColumns: {list(sample_df.columns)}")
print(f"\nFirst 10 rows:")
print(sample_df.tail(30))

# Show statistics
print(f"\n📊 STATISTICS:")
print(sample_df.describe())

# %%
# Merge tất cả vào trục thời gian chuẩn (Left Join)
merged_on_axis = time_index.copy()

for filename, df in monthly_data.items():
    cols_to_merge = [col for col in df.columns if col != 'date']
    if cols_to_merge:
        merge_df = df[['date'] + cols_to_merge].copy()
        
        rows_before = len(merged_on_axis)
        merged_on_axis = merged_on_axis.merge(merge_df, on='date', how='left')
        rows_after = len(merged_on_axis)
        
        print(f"  ✓ {filename:25} | {len(cols_to_merge):2d} features added | {rows_before} → {rows_after} rows")

print(f"Merged dataset shape: {merged_on_axis.shape}")
print(f"Date range: {merged_on_axis['date'].min().date()} to {merged_on_axis['date'].max().date()}")

# %%
merged_on_axis.head()

# %%
# #Imputation với Forward Fill
missing_before = merged_on_axis.isnull().sum().sum()
print(f"  Missing values BEFORE: {missing_before:,}")

# Forward fill only (không back fill để không tạo giá trị tương lai)
merged_on_axis = merged_on_axis.fillna(method='ffill')
# back fill chỉ cho những tháng đầu tiên nếu có null ở đó, vì không có dữ liệu trước đó để ffill
merged_on_axis = merged_on_axis.fillna(method='bfill')
missing_after = merged_on_axis.isnull().sum().sum()
print(f"  Missing values AFTER ffill: {missing_after:,}")

# Show remaining nulls by column
null_cols = merged_on_axis.isnull().sum()
null_cols = null_cols[null_cols > 0].sort_values(ascending=False)

if len(null_cols) > 0:
    print("Remaining nulls by column (at start of time series):")
    for col, count in null_cols.items():
        null_pct = (count / len(merged_on_axis) * 100)
        print(f"{col:40} | {count:4d} nulls ({null_pct:.1f}%)")
else:
    print("\nNo remaining null values!")

print(f"Final dataset: {merged_on_axis.shape[0]} months × {merged_on_axis.shape[1]} columns")
print(f"Data completeness: {(1 - missing_after / (merged_on_axis.shape[0] * merged_on_axis.shape[1])) * 100:.1f}%")

# %%
null_percentage = (merged_on_axis.isnull().sum() / len(merged_on_axis) * 100)
cols_to_drop = null_percentage[null_percentage > 90].index.tolist()

if len(cols_to_drop) > 0:
    print(f"Xóa {len(cols_to_drop)} cột có > 90% null:")
    for col in cols_to_drop:
        null_pct = null_percentage[col]
        print(f"  ✗ {col:40} | {null_pct:.1f}% null → DROP")
    
    merged_on_axis = merged_on_axis.drop(columns=cols_to_drop)
    print(f"\n✓ Sau khi xóa: {merged_on_axis.shape}")
else:
    print("✓ Không có cột nào cần xóa (tất cả ≤ 90% null)")

print(f"\n📊 CẬP NHẬT NULL STATS:")
missing_after_cleanup = merged_on_axis.isnull().sum().sum()
print(f"  Total nulls: {missing_after_cleanup:,}")
print(f"  Data completeness: {(1 - missing_after_cleanup / (merged_on_axis.shape[0] * merged_on_axis.shape[1])) * 100:.1f}%")


# %%
# điền null của ppi_qoq bằng giá trị đầu tiên có giá trị (do nó chỉ có 1 giá trị mỗi quý, nên sẽ lặp lại giá trị đó cho các tháng trong quý)
if 'ppi_qoq' in merged_on_axis.columns:
    first_valid = merged_on_axis['ppi_qoq'].first_valid_index()
    if first_valid is not None:
        first_value = merged_on_axis.loc[first_valid, 'ppi_qoq']
        merged_on_axis['ppi_qoq'] = merged_on_axis['ppi_qoq'].fillna(first_value)
        print(f"✓ Điền null của 'ppi_qoq' bằng giá trị đầu tiên: {first_value}")
    else:
        print("⚠ Không tìm thấy giá trị hợp lệ nào trong 'ppi_qoq' để điền null.")

# %%
merged_on_axis.to_csv(os.path.join(data_processed_dir, 'merged_monthly_data.csv'), index=False)

# %%
merged_on_axis

# %%
# # Lấy đầy đủ tháng + last của tháng 
# monthly_data = {}
# agg_report = []

# for filename, df in loaded_data.items():
#     try:
#         df_agg = df.copy()
#         df_agg['year_month'] = df_agg['date'].dt.to_period('M')
        
#         # Get value columns
#         value_cols = [col for col in df_agg.columns if col not in ['date', 'year_month']]
        
#         # Aggregate (last value of month)
#         agg_dict = {col: 'last' for col in value_cols}
#         monthly = df_agg.groupby('year_month').agg(agg_dict).reset_index()
#         monthly['date'] = monthly['year_month'].dt.to_timestamp()
#         monthly = monthly.drop('year_month', axis=1)
        
#         monthly_data[filename] = monthly
        
#         compression_ratio = (len(monthly) / len(df)) * 100
#         agg_report.append({
#             'Filename': filename,
#             'Original': len(df),
#             'Monthly': len(monthly),
#             'Compression': f"{compression_ratio:.1f}%"
#         })
        
#         print(f"  ✓ {filename:25} | {len(df):6,} rows → {len(monthly):6,} months ({compression_ratio:.1f}% compression)")
    
#     except Exception as e:
#         print(f"  ✗ {filename:25} | ERROR: {str(e)[:40]}")

# print(f"\n✓ Gộp thành công: {len(monthly_data)} files")

# # Show summary
# agg_df = pd.DataFrame(agg_report)
# print(f"\nTổng rows ban đầu: {agg_df['Original'].sum():,}")
# print(f"Tổng tháng sau gộp: {agg_df['Monthly'].sum():,}")

# %% [markdown]
# ## BƯỚC 4: Merge Tất Cả Dữ Liệu

# %%
# print("\n[STEP 3] MERGE TẤT CẢ DỮ LIỆU THEO NGÀY")
# print("="*100)

# # Check CPI data
# if 'cpi_mom.csv' not in monthly_data:
#     print("ERROR: CPI data not found!")
#     raise ValueError("CPI data (cpi_mom.csv) required as base")

# # Start with CPI as base
# cpi_df = monthly_data['cpi_mom.csv'].copy()
# value_col = [col for col in cpi_df.columns if col != 'date'][0]
# base = cpi_df[['date', value_col]].copy()
# base = base.rename(columns={value_col: 'CPI'})
# base = base.sort_values('date').reset_index(drop=True)

# print(f"  Base dataset (CPI): {len(base)} rows from {base['date'].min().date()} to {base['date'].max().date()}")

# # Merge other data
# merged_data = base.copy()
# merge_report = []

# for filename, df in monthly_data.items():
#     if filename != 'cpi_mom.csv':
#         cols_to_merge = [col for col in df.columns if col != 'date']
#         if cols_to_merge:
#             merge_df = df[['date'] + cols_to_merge].copy()
#             rows_before = len(merged_data)
#             merged_data = merged_data.merge(merge_df, on='date', how='outer')
#             rows_after = len(merged_data)
            
#             merge_report.append({
#                 'Source': filename,
#                 'New_Rows': rows_after - rows_before,
#                 'Total_Rows': rows_after,
#                 'Columns_Added': len(cols_to_merge)
#             })
            
#             print(f"  ✓ {filename:25} | +{rows_after - rows_before:4,} rows → {rows_after:6,} total")

# merged_data = merged_data.sort_values('date').reset_index(drop=True)

# print(f"\n✓ Merged dataset: {len(merged_data)} rows × {len(merged_data.columns)} columns")
# print(f"✓ Date range: {merged_data['date'].min().date()} to {merged_data['date'].max().date()}")

# %% [markdown]
# ## BƯỚC 5: Xử Lý Missing Values

# %%
# print("\n[STEP 4] XỬ LÝ MISSING VALUES")
# print("="*100)

# # Filter to CPI data range
# cpi_available = merged_data[merged_data['CPI'].notna()].copy()
# cpi_start = cpi_available['date'].min()
# cpi_end = cpi_available['date'].max()

# print(f"  CPI data range: {cpi_start.date()} to {cpi_end.date()}")

# # Filter to CPI period
# merged_data = merged_data[(merged_data['date'] >= cpi_start) & 
#                           (merged_data['date'] <= cpi_end)].copy()

# print(f"  Filtered to CPI range: {len(merged_data)} rows")

# # Check missing values
# missing_before = merged_data.isnull().sum().sum()
# print(f"  Missing values BEFORE: {missing_before:,}")

# # Fill missing values
# merged_data = merged_data.fillna(method='ffill').fillna(method='bfill')

# missing_after = merged_data.isnull().sum().sum()
# print(f"  Missing values AFTER: {missing_after:,}")

# # Drop all NaN rows
# merged_data = merged_data.dropna(how='all', subset=merged_data.columns[1:])

# print(f"\n✓ Final rows after cleaning: {len(merged_data)}")
# print(f"✓ Data completeness: {(1 - missing_after / (merged_data.shape[0] * merged_data.shape[1])) * 100:.1f}%")

# %% [markdown]
# ## BƯỚC 6: Feature Engineering

# %%
final_data = merged_on_axis.copy()

print(f"  Final dataset for modeling: {len(final_data)} months")
print(f"  Date range: {final_data['date'].min().date()} to {final_data['date'].max().date()}")

final_data['year'] = final_data['date'].dt.year
final_data['month'] = final_data['date'].dt.month
final_data['quarter'] = final_data['date'].dt.quarter

print(f"  ✓ Added temporal features (year, month, quarter)")
print(f"\n  Final shape: {final_data.shape}")
print(f"  Columns: {len(final_data.columns)}")

# %%
final_data.head()

# %% [markdown]
# ## BƯỚC 7: Kiểm Tra Chất Lượng Dữ Liệu

# %%
print("\n[STEP 6] KIỂM TRA CHẤT LƯỢNG DỮ LIỆU")
print("="*100)

print(f"  Final dataset shape: {final_data.shape}")
print(f"  Date range: {final_data['date'].min().date()} to {final_data['date'].max().date()}")
print(f"  Total columns: {len(final_data.columns)}")
print(f"  Numeric columns: {len(final_data.select_dtypes(include=[np.number]).columns)}")
print(f"  Missing values: {final_data.isnull().sum().sum()}")

print("\n📋 CHI TIẾT COLUMNS:")
print("-" * 100)
print(f"{'Column':<30} {'Type':<15} {'Missing':<10} {'Min':<15} {'Max':<15}")
print("-" * 100)

for col in final_data.columns[:15]:  # Show first 15 columns
    dtype = str(final_data[col].dtype)
    missing = final_data[col].isnull().sum()
    
    if final_data[col].dtype in ['float64', 'int64']:
        min_val = f"{final_data[col].min():.2f}"
        max_val = f"{final_data[col].max():.2f}"
    else:
        min_val = str(final_data[col].min())[:15]
        max_val = str(final_data[col].max())[:15]
    
    print(f"{col:<30} {dtype:<15} {missing:<10} {min_val:<15} {max_val:<15}")

# %% [markdown]
# ## BƯỚC 8: Lưu Unified Dataset

# %%
print("\n[STEP 7] SAVING UNIFIED DATASET")
print("="*100)

# Save only the main unified dataset
output_file = os.path.join(data_processed_dir, 'cpi_02_06.csv')
final_data.to_csv(output_file, index=False, encoding='utf-8')
file_size = os.path.getsize(output_file) / 1024  # KB

print(f"  ✓ Saved: cpi_02_06.csv ({len(final_data)} rows, {file_size:.1f} KB)")
print(f"  📁 Location: {output_file}")

# %% [markdown]
# ## BƯỚC 9: Tổng Kết & Thống Kê

# %% [markdown]
# ## Kiểm Tra Lại Kết Quả

# %%
# Verify saved file
if os.path.exists(output_file):
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
    verify_df = pd.read_csv(output_file, nrows=5)
    
    print("✅ VERIFICATION SUCCESSFUL")
    print(f"  ✓ File exists: {output_file}")
    print(f"  ✓ File size: {file_size:.2f} MB")
    print(f"  ✓ Rows: {len(pd.read_csv(output_file))}")
    print(f"  ✓ Columns: {len(verify_df.columns)}")
    print(f"  ✓ Date range: {verify_df['date'].iloc[0]} to {verify_df['date'].iloc[-1]}")
    print("\n✓ Ready for STEP 1: Data Preparation & Exploration")
else:
    print("❌ ERROR: File not saved")


