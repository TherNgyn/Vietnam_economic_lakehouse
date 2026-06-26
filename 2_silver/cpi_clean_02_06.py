import pandas as pd
import numpy as np
import re

df = pd.read_csv(f'./historical_dataset/cpi.csv', encoding='utf-8')

df.rename(columns={df.columns[0]: 'month'}, inplace=True)
print(df)
years = df.columns[1:].tolist()

print(df['month'])
cpi_mom_rows = []

for idx, row in df.iterrows():
    match = re.search(r'(\d{1,2})\s*$', str(row['month']))
    if match:
        month_num = int(match.group(1))
        if 1 <= month_num <= 12:
            for year in years:
                val = row.get(year, np.nan)
                if pd.notnull(val) and val not in ['..', '...']:
                    try:
                        date = f"{year}-{month_num:02d}-01"
                        cpi = float(str(val).replace(',', '.'))
                        cpi_mom_rows.append({
                            'date': date,
                            'year': int(year),
                            'month': month_num,
                            'cpi': cpi
                        })
                    except:
                        continue

cpi_mom = pd.DataFrame(cpi_mom_rows)

if len(cpi_mom) > 0:
    cpi_mom['date'] = pd.to_datetime(cpi_mom['date'])
    cpi_mom = cpi_mom.sort_values(['year', 'month']).reset_index(drop=True)


    # Fill tháng 1 bằng giá trị tháng 2 giống sample
    cpi_mom['inflation'] = cpi_mom['cpi'] - 100
    cpi_mom['inflation'] = cpi_mom.groupby('year')['inflation'].transform(lambda x: x.bfill())

    cpi_mom['avg_year'] = cpi_mom.groupby('year')['cpi'].transform('mean')

    # Tạo dataframe xuất đúng format
    processing_date = pd.Timestamp.today().strftime('%Y-%m-%d')  
    out = pd.DataFrame({
        'date': cpi_mom['date'].dt.strftime('%Y-%m-%d'),
        'indicator': 'cpi_mom',
        'cpi': cpi_mom['cpi'],
        'inflation': cpi_mom['inflation'],
        'avg_year': cpi_mom['avg_year'],
        'unit': 'points',
        'processing_date': processing_date
    })

    # Xuất CSV
    out_path = './historical_dataset/cpi_mom_processed.csv'
    out.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"✅ Saved: {out_path}")
    print(out.head(6))

else:
    print("No CPI Mom data found")
