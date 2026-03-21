from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

url = "https://funan.com.vn/vi/cat/du-lieu-vi-mo_5.html"

options = Options()
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
    wait = WebDriverWait(driver, 20)

    # 1. Click vào tab "Tín dụng"
    menu_links = driver.find_elements(By.CSS_SELECTOR, ".menu_list a.view_report_data")
    for link in menu_links:
        if "Tín dụng" in link.text:
            link.click()
            break
    time.sleep(1)

   
    Select(driver.find_element(By.ID, "cbFromMonth")).select_by_index(0)  # T1
    Select(driver.find_element(By.ID, "cbFromYear")).select_by_index(len(driver.find_elements(By.CSS_SELECTOR, "#cbFromYear option"))-1)  # Năm nhỏ nhất
    Select(driver.find_element(By.ID, "cbToMonth")).select_by_index(11)  # T12
    Select(driver.find_element(By.ID, "cbToYear")).select_by_index(0)  # Năm lớn nhất

  
    view_btn = driver.find_element(By.CSS_SELECTOR, ".btn.btn-view")
    view_btn.click()

    time.sleep(2)

    # Lấy header tháng
    headers = driver.find_elements(By.CSS_SELECTOR, "#HeaderRightSide th")
    months = [h.text.strip() for h in headers if "Tháng" in h.text]

    # Lấy dòng "Cung tiền M2"
    m2_row = driver.find_elements(By.CSS_SELECTOR, "#BodyRightSide tr")[1] 
    values = [td.text.replace(",", "").strip() for td in m2_row.find_elements(By.TAG_NAME, "td") if td.text.strip() and "Xem" not in td.text]

    m2_data = dict(zip(months, values))

    df = pd.DataFrame(list(m2_data.items()), columns=["Month", "M2"])
    df.to_csv("historical_dataset/money_supply_m2.csv", index=False)
    print(df)

finally:
    driver.quit()