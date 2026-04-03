# WTI Crude Oil Futures - Real-time Data Pipeline

## Cấu Trúc Pipeline WTI

Hệ thống thu thập dữ liệu WTI theo thời gian thực theo quy trình sau:

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  1. SCRAPER LAYER (Real-time Data Collection)              │
│  ├─ wti_scraper.py                                         │
│  │  ├─ scrape_wti_investing()  -> Investing.com            │
│  │  ├─ scrape_wti_tradingview() -> TradingView             │
│  │  └─ get_wti_data()           -> Unified interface       │
│                                                             │
│  2. BRONZE LAYER (Raw Data Storage)                        │
│  ├─ ingestion/wti_pipeline.py                              │
│  │  └─ to_bronze()              -> S3/MinIO store          │
│  │     Path: bronze/commodity/wti/YYYY-MM-DD/             │
│                                                             │
│  3. SILVER LAYER (Data Cleaning & Validation)             │
│  ├─ silver/wti_clean.py                                    │
│  │  ├─ clean_wti_data()      -> Data transformation       │
│  │  └─ validate_wti_schema() -> Quality check             │
│  │     Path: silver/commodity/wti/YYYY-MM-DD/             │
│                                                             │
│  4. ORCHESTRATION (Airflow DAG)                            │
│  └─ dags/wti_daily_dag.py                                  │
│     Schedule: 9am, 2pm, 5pm (Weekdays)                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Thành Phần Chính

### 1. Scraper (`ingestion/scrapers/wti_scraper.py`)

**Chức năng:**
- Scrape real-time WTI price từ Investing.com hoặc TradingView
- Parse HTML sử dụng Selenium
- Extract: price, change, change_percent, timestamp

**Data Output:**
```json
{
  "timestamp": "2026-03-31 17:16:25",
  "contract": "WTI_MAY2026",
  "symbol": "OIL",
  "currency": "USD",
  "price": 104.33,
  "change": 1.45,
  "change_percent": 1.41,
  "source": "investing.com",
  "data_type": "real-time"
}
```

**Sử dụng:**
```python
from ingestion.scrapers.wti_scraper import get_wti_data

# Scrape from Investing.com
data = get_wti_data(source="investing")

# Or from TradingView
data = get_wti_data(source="tradingview")
```

### 2. Silver Cleaning (`silver/wti_clean.py`)

**Chức năng:**
- Type conversion & validation
- Data quality checks
- Enrich với derived columns
- Schema enforcement

**Transformed Columns:**
```
- timestamp, date, time, hour, minute (time breakdowns)
- price, previous_price (computed)
- change, price_abs_change, change_percent
- processed_at, data_quality_score
```

**Sử dụng:**
```python
from silver.wti_clean import clean_wti_data

df_raw = pd.DataFrame([raw_data])
df_clean = clean_wti_data(df_raw)
```

### 3. Pipeline Integration (`ingestion/wti_pipeline.py`)

**Chức năng:**
- Unified interface cho toàn bộ pipeline
- Quản lý paths & partitions
- Error handling

**Sử dụng - Phương pháp 1 (Recommended):**
```python
from ingestion.wti_pipeline import WTIPipeline

# Initialize
pipeline = WTIPipeline(
    bronze_path='./data/bronze',
    silver_path='./data/silver'
)

# Run complete pipeline
result = pipeline.run(source='investing')

print(result)
# Output:
# {
#   'status': 'success',
#   'price': 104.33,
#   'timestamp': '2026-03-31 17:16:25',
#   'bronze_path': './data/bronze/commodity/wti/2026-03-31/...',
#   'silver_path': './data/silver/commodity/wti/2026-03-31/...'
# }
```

**Sử dụng - Phương pháp 2 (Step by step):**
```python
pipeline = WTIPipeline()

# Step 1: Scrape
data = pipeline.scrape(source='investing')

# Step 2: Store raw to bronze
bronze_path = pipeline.to_bronze(data)

# Step 3: Transform to silver
silver_path = pipeline.to_silver(data)
```

### 4. Airflow DAG (`dags/wti_daily_dag.py`)

**Schedule:**
- **Time**: 9am, 2pm, 5pm (UTC time)
- **Days**: Weekdays chỉ (Mon-Fri)
- **ID**: `wti_daily_pipeline`

**Workflow:**
```
scrape_wti → ingest_to_bronze → transform_to_silver → validate_and_notify
```

**Deploy DAG:**
```bash
# Copy DAG to Airflow dags folder
cp dags/wti_daily_dag.py $AIRFLOW_HOME/dags/

# Trigger in UI or CLI
airflow dags trigger wti_daily_pipeline
```

## Data Storage Schema

### Bronze Layer Partitioning
```
s3://bronze/commodity/wti/2026-03-31/
├── wti_raw_17_171625.parquet  (17:16:25)
├── wti_raw_14_140000.parquet  (14:00:00)
└── wti_raw_09_090000.parquet  (09:00:00)
```

### Silver Layer Partitioning
```
s3://silver/commodity/wti/2026-03-31/
├── wti_cleaned_171625.parquet (17:16:25)
├── wti_cleaned_140000.parquet (14:00:00)
└── wti_cleaned_090000.parquet (09:00:00)
```

## Installation & Dependencies

### Requirements:
```
selenium>=4.0.0
webdriver-manager>=3.8.0
pandas>=1.3.0
apache-airflow>=2.0.0
pyarrow>=10.0.0
```

### Install:
```bash
pip install selenium webdriver-manager pandas apache-airflow pyarrow
```

## Running WTI Pipeline

### Option 1: Direct Python (Testing)
```bash
cd Vietnam_economic_lakehouse

# Run scraper only
python -m ingestion.scrapers.wti_scraper

# Run full pipeline
python -m ingestion.wti_pipeline
```

### Option 2: Airflow (Production)
```bash
# List DAGs
airflow dags list

# Trigger manually
airflow dags trigger wti_daily_pipeline

# Monitor execution
airflow dags list-runs wti_daily_pipeline
```

### Option 3: Docker (Recommended)
```bash
# Build image
docker-compose build

# Run DAG in container
docker-compose up airflow-scheduler
docker-compose up airflow-worker
```

## Monitoring & Troubleshooting

### Check Airflow UI
```
http://localhost:8080
- DAG: wti_daily_pipeline
- Monitor tasks status
- Check logs
```

### View Data Files
```bash
# List bronze files
ls data/bronze/commodity/wti/2026-03-31/

# Read parquet file
python -c "import pandas as pd; print(pd.read_parquet('data/bronze/commodity/wti/2026-03-31/wti_raw_17_171625.parquet'))"
```

### Troubleshoot Scraper
```bash
python -c "
from ingestion.scrapers.wti_scraper import scrape_wti_investing
data = scrape_wti_investing()
print(data)
"
```

## Next Steps

1. **Update MinIO/S3 Configuration**
   - Modify paths in `wti_pipeline.py` for S3 storage
   - Add credentials in Airflow variables

2. **Add Database Sink**
   - Modify `wti_daily_dag.py` task_validate to write to PostgreSQL/ClickHouse
   - Add downstream queries for analytics

3. **Set Up Alerts**
   - Add price anomaly detection
   - Email/Slack notifications for large price movements

4. **Expand Data Sources**
   - Add more financial websites
   - Fallback sources if main source fails
   - Compare prices across sources

## Troubleshooting

### Scraper not finding elements
- Check website HTML structure hasn't changed
- Update CSS selectors in `wti_scraper.py`
- Add `--headless=new` to see what's happening

### DAG not triggering
- Check Airflow scheduler is running
- Verify cron schedule syntax
- Check logs: `airflow dags list-runs wti_daily_pipeline`

### Data quality issues
- Enable verbose logging in `wti_clean.py`
- Check `data_quality_score` in silver layer
- Review `change_percent` calculations

---

**Tác giả**: Vietnam Economic Lakehouse  
**Cập nhật**: 2026-03-31  
**Phiên bản**: 1.0
