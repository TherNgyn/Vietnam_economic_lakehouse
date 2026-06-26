import pandas as pd
import numpy as np
from datetime import datetime
import re

df = pd.read_csv('./historical_dataset/product_world/gasoline_prices.csv')
def standardize_source(source):
    source_lower = str(source).lower()
    if 'pvoil' in source_lower:
        return 'PVOIL'
    elif 'petrolimex' in source_lower:
        return 'Petrolimex'
    return source
df['source'] = df['source'].apply(standardize_source)
def clean_oil_name(name):
    name = name.upper()
    name = re.sub(r'(\d+[,.]\d+%?S(-\w+)?|[- ]+[IVXLC]+$)', '', name)
    if 'XĂNG E5' in name: return 'E5 RON 92'
    if 'RON 95' in name: return 'RON 95'
    if 'DẦU DO' in name: return 'DO'
    if 'DẦU FO' in name: return 'FO'
    if 'DẦU KO' in name: return 'KO'
    return name.strip()

def clean_type(name):
    if 'Xăng' in name: return 'Gasoline'
    if 'Dầu FO' in name: return 'Fuel Oil'
    if 'Dầu KO' in name: return 'Kerosene'
    if 'Dầu' in name: return 'Diesel'
    return 'Other'

gasoline_rows = []
for idx, row in df.iterrows():
    date_str = row['date'].strip()
    type = clean_type(row['product'])
    product = clean_oil_name(row['product'])
    price = row['price']
    change = row['change']
    unit = row['unit'].strip()
    source = row['source'].strip()

    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
    date = date_obj.strftime('%Y-%m-%d')
    
    gasoline_rows.append({
        'date': date,
        'type': type,
        'product': product,
        'price': price,
        'change': change,
        'unit': unit,
        'source': source
    })

gasoline = pd.DataFrame(gasoline_rows)
gasoline = gasoline.sort_values('date').reset_index(drop=True)

print(gasoline.head(20))
print(gasoline.tail(10))