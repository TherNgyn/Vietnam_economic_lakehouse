from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

url = "https://www.pvoil.com.vn/tin-gia-xang-dau"  

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

        # Lấy ngày mới nhất từ dropdown
        # select = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlpricedate"))))
        # date_text = select.options[0].text.strip()
        # select.select_by_index(0)
        # time.sleep(5)  # Đợi bảng cập nhật

        select = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlpricedate"))))
        num_options = len(select.options)
        data = []

        for i in range(num_options):
 
            select = Select(driver.find_element(By.ID, "ddlpricedate"))
            date_text = select.options[i].text.strip()
            select.select_by_index(i)
            time.sleep(3) 

            table = driver.find_element(By.CSS_SELECTOR, "div.oilpricescontainer table")
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) == 4:
                    item = cols[1].text.strip()
                    price = cols[2].text.strip().replace(" đ", "").replace(".", "").replace(",", "")
                    change = cols[3].text.strip()
                    data.append({
                        "date": date_text,
                        "product": item,
                        "price": price,
                        "change": change,
                        "unit": "VND/liter",
                        "source": url,
                        "time_scraped": pd.Timestamp.now()
                    })
        df = pd.DataFrame(data)
        df.to_csv("historical_dataset/gasoline_prices.csv", index=False)
        print(df) 

finally:
    driver.quit()