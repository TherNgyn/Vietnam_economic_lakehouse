import pandas as pd
import numpy as np

df = pd.read_csv('./historical_dataset/vietnam-producer-price-inflation-qoq.csv')
def extract_quarter_to_date(quarter_str):
    year, q = quarter_str.split()[1], quarter_str.split()[0][1]
    month = (int(q) - 1) * 3 + 1
    return f"{year}-{month:02d}-01"
    
ppi_rows = []
for idx, row in df.iterrows():
    quarter = row['QUARTER'].strip()
    ppi_str = row['PRODUCER PRICE INFLATION QOQ'].strip()

    parts = quarter.split()
    q = parts[0] 
    year = parts[1]  
    date = extract_quarter_to_date(quarter)

    ppi_value = float(ppi_str.replace('%', ''))
    quarter = f"{year}-Q{q[-1]}"
    ppi_rows.append({
        'date': date,
        'quarter': quarter,
        'ppi_qoq': ppi_value
    })

ppi_qoq = pd.DataFrame(ppi_rows)
ppi_qoq = ppi_qoq.sort_values('date').reset_index(drop=True)

ppi_qoq = ppi_qoq[['date', 'quarter', 'ppi_qoq']]
ppi_qoq['unit'] = '%'

print(ppi_qoq.head(20))
