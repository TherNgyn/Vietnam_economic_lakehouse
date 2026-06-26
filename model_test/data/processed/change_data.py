import pandas as pd
import numpy as np
from pathlib import Path
import html


BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / 'cpi_02_06.csv'
OUTPUT_FILE = BASE_DIR / 'cpi_02_06_pct_change.csv'


def convert_value_cols_to_pct_change(input_file, output_file):
    df = pd.read_csv(input_file)

    # Decode tên cột nếu có HTML entity: S&amp;P500 -> S&P500
    df.columns = [html.unescape(col) for col in df.columns]

    # Chuẩn hóa date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Nếu chưa có year/month/quarter thì tạo
    if 'year' not in df.columns:
        df['year'] = df['date'].dt.year

    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month

    if 'quarter' not in df.columns:
        df['quarter'] = df['date'].dt.quarter

    # Các cột đã là % hoặc rate, giữ nguyên
    rate_cols = [
        'cpi_mom_processed_inflation',
        'core_inflation_rate',
        'interest_rate',
        'ppi_qoq',
        'broad_money',
        'policy_rate'
    ]

    # Các cột monthly value/index/level, tính MoM % change
    monthly_value_cols = [
        'cpi_mom_processed_cpi',
        'm2',
        'brent',
        'wti',
        'gasoline_world',
        'natural_gas',
        'gold',
        'silver',
        'VNINDEX',
        'VN30',
        'HNX',
        'UPCOM',
        'NASDAQ',
        'S&P500',
        'DAX',
        'DOWJONES',
        'NIKKEI225',
        'HANGSENG',
        'USDVND'
    ]

    # Các cột level theo quý, tính QoQ % change
    quarterly_value_cols = [
        'gdp'
    ]

    # Chỉ lấy các cột thật sự tồn tại
    rate_cols = [col for col in rate_cols if col in df.columns]
    monthly_value_cols = [col for col in monthly_value_cols if col in df.columns]
    quarterly_value_cols = [col for col in quarterly_value_cols if col in df.columns]

    # Convert numeric
    numeric_cols = rate_cols + monthly_value_cols + quarterly_value_cols

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # =====================================================
    # 1. TÍNH MoM % CHANGE CHO CÁC CỘT MONTHLY
    # =====================================================
    for col in monthly_value_cols:
        df[f'{col}_pct_change'] = df[col].pct_change() * 100

    monthly_pct_change_cols = [
        f'{col}_pct_change'
        for col in monthly_value_cols
    ]

    # =====================================================
    # 2. TÍNH QoQ % CHANGE CHO CÁC CỘT QUARTERLY, VD: GDP
    # =====================================================
    quarterly_pct_change_cols = []

    if len(quarterly_value_cols) > 0:
        quarter_df = (
            df
            .sort_values('date')
            .groupby(['year', 'quarter'], as_index=False)
            .agg({
                col: 'last' for col in quarterly_value_cols
            })
        )

        quarter_df = quarter_df.sort_values(['year', 'quarter']).reset_index(drop=True)

        for col in quarterly_value_cols:
            new_col = f'{col}_qoq_pct_change'
            quarter_df[new_col] = quarter_df[col].pct_change() * 100
            quarterly_pct_change_cols.append(new_col)

        # Chỉ giữ key + cột QoQ change để merge lại vào monthly data
        merge_cols = ['year', 'quarter'] + quarterly_pct_change_cols

        df = df.merge(
            quarter_df[merge_cols],
            on=['year', 'quarter'],
            how='left'
        )

    # =====================================================
    # 3. XỬ LÝ INF
    # =====================================================
    all_pct_cols = monthly_pct_change_cols + quarterly_pct_change_cols

    if len(all_pct_cols) > 0:
        df[all_pct_cols] = df[all_pct_cols].replace(
            [np.inf, -np.inf],
            np.nan
        )

    # =====================================================
    # 4. BỎ RAW VALUE CỦA CÁC CỘT ĐÃ CHUYỂN ĐỔI
    # =====================================================
    drop_cols = monthly_value_cols + quarterly_value_cols
    df_output = df.drop(columns=drop_cols)

    # Đổi tên MoM pct_change về tên gốc
    # Ví dụ: brent_pct_change -> brent
    monthly_rename_dict = {
        f'{col}_pct_change': col
        for col in monthly_value_cols
    }

    # Đổi tên QoQ pct_change
    # Ví dụ: gdp_qoq_pct_change -> gdp
    quarterly_rename_dict = {
        f'{col}_qoq_pct_change': col
        for col in quarterly_value_cols
    }

    rename_dict = {}
    rename_dict.update(monthly_rename_dict)
    rename_dict.update(quarterly_rename_dict)

    df_output = df_output.rename(columns=rename_dict)

    # Sort lại theo date
    df_output = df_output.sort_values('date').reset_index(drop=True)

    # Format date
    df_output['date'] = df_output['date'].dt.strftime('%Y-%m-%d')

    # Lưu file
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df_output.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f'Đã lưu file: {output_file}')
    print(f'Số dòng: {len(df_output)}')
    print(f'Số cột: {len(df_output.columns)}')
    print(f'Cột monthly đã chuyển sang MoM % change: {monthly_value_cols}')
    print(f'Cột quarterly đã chuyển sang QoQ % change: {quarterly_value_cols}')
    print(f'Cột giữ nguyên vì đã là rate/%: {rate_cols}')

    return df_output


if __name__ == '__main__':
    convert_value_cols_to_pct_change(INPUT_FILE, OUTPUT_FILE)