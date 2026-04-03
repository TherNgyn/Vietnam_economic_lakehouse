import pandas as pd
import numpy as np
from datetime import datetime

df = pd.read_csv('./historical_dataset/vietnam-interest-rate.csv')

def standardize_source(source):
    source_lower = str(source).lower()
    if 'sbv' in source_lower or 'gov' in source_lower or 'vn' in source_lower:
        return 'Viet Nam Central Bank'
    return source

df['source'] = df['source'].apply(standardize_source)
df.isnull().sum()
print(df.isnull().sum())

# Điền null term bằng 'Unknown'
df['term'] = df['term'].fillna('Unknown')
# in ra dòng nào có giá trị null
print(df[df.isnull().any(axis=1)])


term_mapping = {
    'Qua đêm': ('Overnight', 'ON'),
    '1 Tuần': ('1 Week', '1W'),
    '2 Tuần': ('2 Weeks', '2W'),
    '1 Tháng': ('1 Month', '1M'),
    '2 Tháng': ('2 Months', '2M'),
    '3 Tháng': ('3 Months', '3M'),
    '6 Tháng': ('6 Months', '6M'),
    '9 Tháng': ('9 Months', '9M'),
    '12 Tháng': ('12 Months', '12M'),
}

def parse_date(date_str):
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    return date_obj.strftime('%Y-%m-%d')

interest_rate_rows = []
for idx, row in df.iterrows():
    date_str = row['date'].strip()
    term_str = row['term'].strip()
    rate_str = row['interest_rate'].strip()
    volume = row['volume']
    source = row['source'].strip()
    
    date = parse_date(date_str)

    rate = float(rate_str.replace(',', '.'))

    if term_str in term_mapping:
        term_en, symbol = term_mapping[term_str]
    else:
        term_en = term_str
        symbol = term_str
    
    interest_rate_rows.append({
        'date': date,
        'term': term_en,
        'symbol': symbol,
        'interest_rate': rate,
        'volume': volume,
        'source': source
    })

interest_rate = pd.DataFrame(interest_rate_rows)
interest_rate = interest_rate.sort_values(['date', 'symbol']).reset_index(drop=True)

print(interest_rate.head(20))
print(interest_rate.tail(10))