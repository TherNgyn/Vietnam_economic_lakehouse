from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from tomlkit import datetime
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    wait = WebDriverWait(driver, 20)

    from_date = "03/26/2026"
    to_date = datetime.now().strftime("%m/%d/%Y")

    from_input = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
    to_input = driver.find_element(By.ID, "toDate")

    from_input.clear()
    from_input.send_keys(from_date)
    to_input.clear()
    to_input.send_keys(to_date)
    driver.find_element(By.ID, "btnSearch").click()
    time.sleep(10)

    for page in range(1, 2):
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for i in range(len(rows)):
              
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                row = rows[i]
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text
                    links = cols[1].find_elements(By.TAG_NAME, "a")
                    if links:
                        driver.execute_script("arguments[0].click();", links[0])
                        time.sleep(1)
                        # Lấy bảng chi tiết
                        detail_rows = driver.find_elements(By.CSS_SELECTOR, "#detail-view table tbody tr")
                        for drow in detail_rows:
                            dcols = drow.find_elements(By.TAG_NAME, "td")
                            if len(dcols) == 3:
                                all_data.append({
                                    "date": date_text,
                                    "term": dcols[0].text.strip(),
                                    "interest_rate": dcols[1].text.strip(),
                                    "volume": dcols[2].text.strip(),
                                    "source": url,
                                    "time_scraped": pd.Timestamp.now()
                                })
                        time.sleep(1)
                        # Quay lại
                        back_btn = driver.find_element(By.ID, "btn-back")
                        driver.execute_script("arguments[0].click();", back_btn)
                        time.sleep(1)
            # Sau khi xử lý hết các dòng, chuyển trang nếu còn
            try:
                next_btn = driver.find_element(By.XPATH, "//div[@id='pagination']//button[contains(text(),'Sau')]")
                if next_btn.get_attribute("disabled"):
                    break
                driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(2)
            except Exception:
                break

    # 3. Xuất ra file CSV
    df = pd.DataFrame(all_data)
    df.to_csv("historical_dataset/vietnam-interest-rate.csv", index=False, encoding="utf-8-sig", )
    print(df)

finally:
    driver.quit()