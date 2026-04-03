import pandas as pd
import numpy as np
import re

df = pd.read_csv('./historical_dataset/cpi.csv', encoding='cp1258')

df.rename(columns={df.columns[0]: 'month'}, inplace=True)
print(df)
years = df.columns[1:].tolist()

# Tạo bảng cpi_mom   
months = [f'{i:02d}' for i in range(1, 13)]
print (df['month'])
cpi_mom_rows = []

for idx, row in df.iterrows():  
    match = re.match(r'Tháng (\d+)', str(row['month']))
    if match:
        month_num = match.group(1)
        for i, year in enumerate(years):
            if i < len(row) - 1 and pd.notnull(row[year]) and row[year] not in ['..', '...']:
                date = f"{year}-{int(month_num):02d}"
                cpi = float(str(row[year]).replace(',', '.'))
                cpi_mom_rows.append({'date': date, 'year': int(year), 'month': int(month_num), 'cpi': cpi})

cpi_mom = pd.DataFrame(cpi_mom_rows)
cpi_mom = cpi_mom.sort_values(['year', 'month']).reset_index(drop=True)
cpi_mom['inflation'] = cpi_mom.groupby('year')['cpi'].transform(lambda x: x.diff() / x.shift(1) * 100)
cpi_mom['avg_year'] = cpi_mom.groupby('year')['cpi'].transform('mean')
# Tạo bảng cpi_base_year

def get_row_by_label(label):
    return df[df['month'].str.contains(label, case=False, na=True)].iloc[0]


row_prev_year = get_row_by_label('=100')
row_2000 = get_row_by_label('Nam 2000')
row_2005 = get_row_by_label('Nam 2005')
row_2010 = get_row_by_label('Nam 2010')

cpi_base_year_rows = []
for i, year in enumerate(years):
    prev_year_base = row_prev_year[year]
    base_2000 = row_2000[year]
    base_2005 = row_2005[year]
    base_2010 = row_2010[year]
    if all(x for x in [prev_year_base, base_2000, base_2005, base_2010]):
        cpi_base_year_rows.append({
            'date': year,
            'prev_year_base': float(str(prev_year_base).replace('..', 'nan').replace(',', '.')),
            '2000_base': float(str(base_2000).replace('..', 'nan').replace(',', '.')),
            '2005_base': float(str(base_2005).replace('..', 'nan').replace(',', '.')),
            '2010_base': float(str(base_2010).replace('..', 'nan').replace(',', '.'))
        })

cpi_base_year = pd.DataFrame(cpi_base_year_rows)

cpi_base_year = cpi_base_year.replace('..', np.nan)

# Chỉ tính prev_year_base cho những năm bị null
annual_avg = cpi_mom[['year', 'avg_year']].drop_duplicates().reset_index(drop=True)
annual_avg['prev_year_avg_shift'] = annual_avg['avg_year'].shift(1)
annual_avg['prev_year_base_calc'] = (annual_avg['avg_year'] / annual_avg['prev_year_avg_shift']) * 100

# Chỉ fill NaN của prev_year_base
null_mask = cpi_base_year['prev_year_base'].isna()
cpi_base_year.loc[null_mask, 'prev_year_base'] = cpi_base_year.loc[null_mask, 'date'].astype(int).map(dict(zip(annual_avg['year'], annual_avg['prev_year_base_calc'])))

# Tương tự cho các cột khác
for col in ['2000_base', '2005_base', '2010_base']:
    first_valid = cpi_base_year[col].dropna().iloc[0] if len(cpi_base_year[col].dropna()) > 0 else 100.0
    cpi_base_year.loc[cpi_base_year[col].isna(), col] = first_valid


print(cpi_mom.head(20))
print(cpi_base_year.head(10))