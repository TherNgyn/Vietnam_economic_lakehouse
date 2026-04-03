import pandas as pd
import numpy as np
from datetime import datetime

df = pd.read_csv('./historical_dataset/money_supply_m2.csv')
def standardize_source(source):
    source_lower = str(source).lower()
    if 'funan' in source_lower:
        return 'Funan Research Institute'
    return source

df['source'] = df['source'].apply(standardize_source)

def parse_month(month_str):
    # "Tháng 1/2026" -> "2026-01-01"
    parts = month_str.replace('Tháng ', '').split('/')
    month = int(parts[0])
    year = int(parts[1])
    return f"{year:04d}-{month:02d}-01"

m2_rows = []
for idx, row in df.iterrows():
    month_str = row['month'].strip()
    m2_value = row['m2']
    unit = row['unit'].strip()
    source = row['source'].strip()
    
    
    date = parse_month(month_str)
    
    m2_rows.append({
        'date': date,
        'm2': m2_value,
        'unit': unit,
        'source': source
    })

m2 = pd.DataFrame(m2_rows)
m2 = m2.sort_values('date').reset_index(drop=True)

print(m2.head(20))
print(m2.tail(10))