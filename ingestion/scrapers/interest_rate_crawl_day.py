from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime

url = "https://www.sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t-th%E1%BB%8B-tr%C6%B0%E1%BB%9Dng-li%C3%AAn-ng%C3%A2n-h%C3%A0ng"

options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

all_data = []

try:
    driver.get(url)
    time.sleep(7) 

    today = datetime.now().strftime("%d-%m-%Y")

    table = driver.find_element(By.ID, "new-information-view")
    rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 3:
            all_data.append({
                "date": today,
                "term": cols[0].text.strip(),
                "interest_rate": cols[1].text.strip(),
                "volume": cols[2].text.strip(),
                "source": url,
                "time_scraped": pd.Timestamp.now()
            })

    df = pd.DataFrame(all_data)
    df.to_csv(f"./historical_dataset/vietnam-interest-rate-{today}.csv", index=False, encoding="utf-8-sig")
    print(df)

finally:
    driver.quit()