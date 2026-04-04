import pandas as pd
import numpy as np

df = pd.read_csv('./historical_dataset/vietnam-core-inflation-rate.csv')

def parse_month(month_str):
    parts = month_str.replace('Thg', '').split('-')
    month = int(parts[0])
    year = int(parts[1])
    if year >= 0 and year <= 30:
        year = 2000 + year
    return f"{year:04d}-{month:02d}-01"

core_inflation_rows = []
for idx, row in df.iterrows():
    month_str = row['MONTH'].strip()
    rate_str = row['CORE INFLATION RATE'].strip()
    
    date = parse_month(month_str)
    rate = float(rate_str.replace('%', ''))
    
    core_inflation_rows.append({
        'date': date,
        'core_inflation_rate': rate,
        'unit': '%'
    })

core_inflation = pd.DataFrame(core_inflation_rows)
core_inflation = core_inflation.sort_values('date').reset_index(drop=True)

print(core_inflation.head(20))
print(core_inflation.tail(10))