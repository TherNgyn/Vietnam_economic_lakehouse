# from vnstock import *
# # load Index data
# # world
# index = Vnstock().world_index(symbol = 'INX', source='MSN')

# df = index.quote.history(start='2026-01-01', end='2026-03-14',interval= '1D')
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

url = "https://banggia.vikkibanks.vn/UPCOM-IDX?lang=vi"

options = Options()
# options.add_argument('--headless=new') 
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

try:
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    price = wait.until(
        EC.presence_of_element_located((By.ID, "txtIndex_100"))
    ).text

    change = driver.find_element(By.ID, "txtChangePoint_100").text
    percent = driver.find_element(By.ID, "txtChangePerCent_100").text
    volume = driver.find_element(By.ID, "txtQty_100").text
    value = driver.find_element(By.ID, "txtAmt_100").text

    adv = driver.find_element(By.ID, "txtAdvances_100").text
    nochange = driver.find_element(By.ID, "txtNochanges_100").text
    dec = driver.find_element(By.ID, "txtDeclines_100").text

    status = driver.find_element(By.CLASS_NAME, "MktStatus").text

    # code clean cho silver
    def parse_float(x):
        return float(x.replace(",", ""))

    data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "index": "VNINDEX",
        "price": parse_float(price),
        "change": parse_float(change),
        "percent": parse_float(percent),
        "volume": parse_float(volume) * 1_000_000,
        "value": parse_float(value) * 1_000_000_000,
        "advancers": int(adv),
        "unchanged": int(nochange),
        "decliners": int(dec),
        "status": status
    }

    print(data)

finally:
    driver.quit()