import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
# mã vn30: txtIndex_101,txtChangePoint_101
# mã hnx: txtIndex_200
# mã hnx_30: txtIndex_201
# mã upcome: txtIndex_300
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

def crawl_vikkibanks_all():
    url = "https://banggia.vikkibanks.vn/UPCOM-IDX?lang=vi"
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    results = {}
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        for name, idx in VIKKIBANKS_INDEX_IDS.items():
            try:
                price = wait.until(EC.presence_of_element_located((By.ID, f"txtIndex_{idx}"))).text
                symbol = driver.find_element(By.ID, f"txtIndex_{idx}").text
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
                    "index": symbol,
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
# HNX: index-market-2
# UPCOM: index-market-3
# Vn30: index-market-4
# hnx30: index-market-5
VIETSTOCK_INDEX_IDS = {
    "VNINDEX": "1",
    "VN30": "4",
    "HNX": "2",
    "HNX30": "5",
    "UPCOM": "3"
}

def crawl_vietstock_all():
    url = "https://banggia.vietstock.vn/bang-gia/upcom"
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    results = {}
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        for name, idx in VIETSTOCK_INDEX_IDS.items():
            try:
                price = wait.until(EC.presence_of_element_located((By.ID, f"index-close-{idx}"))).get_attribute("data-value")
                symbol = driver.find_element(By.ID, f"index-market-{idx}").text
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
                    "index": symbol,
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

if __name__ == "__main__":
    try:
        data = crawl_vikkibanks_all()
        print("Vikkibanks results:")
        for k, v in data.items():
            print(f"{k}: {v}")
    except Exception as e:
        print("Vikkibanks failed, fallback to vietstock:", e)
        try:
            data = crawl_vietstock_all()
            print("Vietstock results:")
            for k, v in data.items():
                print(f"{k}: {v}")
        except Exception as e2:
            print("Vietstock also failed:", e2)