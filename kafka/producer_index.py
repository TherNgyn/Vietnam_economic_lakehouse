import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "index-realtime"

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3
)

def parse_float(x):
    if not x:
        return 0.0
    x = x.replace(",", "").replace(" ", "").replace("\u2212", "-")
    return float(x)

VIKKIBANKS_INDEX_IDS = {
    "VNINDEX": "100",
    "VN30": "101",
    "HNX": "200",
    "HNX30": "201",
    "UPCOM": "300"
}

VIETSTOCK_INDEX_IDS = {
    "VNINDEX": "1",
    "VN30": "4",
    "HNX": "2",
    "HNX30": "5",
    "UPCOM": "3"
}

def crawl_vikkibanks_all():
    url = "https://banggia.vikkibanks.vn/UPCOM-IDX?lang=vi"
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--headless=new')
    
    driver = webdriver.Chrome(
        service=Service('/usr/bin/chromedriver'),
        options=options
    )
    results = {}
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        for name, idx in VIKKIBANKS_INDEX_IDS.items():
            try:
                price = wait.until(EC.presence_of_element_located((By.ID, f"txtIndex_{idx}"))).text
                # symbol = driver.find_element(By.ID, f"txtIndex_{idx}").text
                change = driver.find_element(By.ID, f"txtChangePoint_{idx}").text
                percent = driver.find_element(By.ID, f"txtChangePerCent_{idx}").text
                volume = driver.find_element(By.ID, f"txtQty_{idx}").text
                value = driver.find_element(By.ID, f"txtAmt_{idx}").text
                adv = driver.find_element(By.ID, f"txtAdvances_{idx}").text
                nochange = driver.find_element(By.ID, f"txtNochanges_{idx}").text
                dec = driver.find_element(By.ID, f"txtDeclines_{idx}").text
                status = driver.find_element(By.CLASS_NAME, "MktStatus").text

                data = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "index": name,
                    "price": parse_float(price),
                    "change": parse_float(change),
                    "percent": parse_float(percent),
                    "volume": parse_float(volume),
                    "value": parse_float(value),
                    "unit": "Points",
                    "advancers": int(adv),
                    "unchanged": int(nochange),
                    "decliners": int(dec),
                    "status": status,
                    "source": "vikkibanks"
                }
                results[name] = data
            except Exception as e:
                results[name] = {"error": str(e)}
        return results
    finally:
        driver.quit()

def crawl_vietstock_all():
    url = "https://banggia.vietstock.vn/bang-gia/upcom"
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--headless=new')
    
    driver = webdriver.Chrome(
        service=Service('/usr/bin/chromedriver'),
        options=options
    )
    results = {}
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        for name, idx in VIETSTOCK_INDEX_IDS.items():
            try:
                price = wait.until(EC.presence_of_element_located((By.ID, f"index-close-{idx}"))).get_attribute("data-value")
                # symbol = driver.find_element(By.ID, f"index-market-{idx}").text
                change_text = driver.find_element(By.ID, f"index-change-{idx}").text.strip()
                if "(" in change_text and ")" in change_text:
                    change_val, change_pct = change_text.split("(")
                    change = parse_float(change_val)
                    percent = parse_float(change_pct.replace("%", "").replace(")", ""))
                else:
                    change = parse_float(change_text)
                    percent = None
                volume = driver.find_element(By.ID, f"index-totalVol-{idx}").get_attribute("data-value")
                value = driver.find_element(By.ID, f"index-totalVal-{idx}").get_attribute("data-value")
                adv = driver.find_element(By.ID, f"index-advances-{idx}").text
                nochange = driver.find_element(By.ID, f"index-noChange-{idx}").text
                dec = driver.find_element(By.ID, f"index-declines-{idx}").text

                data = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "index": name,
                    "price": parse_float(price),
                    "change": change,
                    "percent": percent,
                    "volume": parse_float(volume),
                    "value": parse_float(value),
                    "unit": "Points",
                    "advancers": int(adv),
                    "unchanged": int(nochange),
                    "decliners": int(dec),
                    "status": None,
                    "source": "vietstock"
                }
                results[name] = data
            except Exception as e:
                results[name] = {"error": str(e)}
        return results
    finally:
        driver.quit()

def fetch_and_send_indices():
    try:
       
        results = crawl_vikkibanks_all()
        if not results or all(v.get("error") for v in results.values()):
            results = crawl_vietstock_all()

        for name, data in results.items():
            if "error" not in data:
                data['timestamp'] = datetime.now(timezone.utc).isoformat()
                kafka_producer.send(KAFKA_TOPIC, value=data)
                kafka_producer.flush()
                print(f"[{name}] {data['price']} Points → Kafka")
        
        return True
    except Exception as e:
        print(f"Error fetching indices: {e}")
        return False

def run_producer():
    print("Starting Producer (vikkibanks/vietstock → Kafka)")
    print("Interval: 1 minute\n")
    
    iteration = 0
    while True:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{timestamp}] Iteration {iteration}")
        
        fetch_and_send_indices()
        
        print(f"Waiting 60 seconds...", end='', flush=True)
        time.sleep(60)
        print(" Done!")

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n\nProducer stopped.")
        kafka_producer.close()