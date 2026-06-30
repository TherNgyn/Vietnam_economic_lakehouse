import time
import json
import logging
from datetime import datetime
from typing import Dict, Any

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# import file cur.yaml 
import yaml
with open("ingestion/scrapers/cur.yaml", "r", encoding="utf-8") as f:
    CUR_CONFIG = yaml.safe_load(f)

cur_list = CUR_CONFIG.get("currencies", [])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCES = {
    "tradingview": "https://www.tradingview.com/symbols/{symbol}/?exchange=FXPRO",
    "investing": "https://vn.investing.com/currencies/{slug}"
}

def parse_number(text: str) -> float:
    if not text:
        return 0.0
    # Xử lý dấu âm unicode và dấu phẩy
    text = text.replace("−", "-").replace(",", "")
    return float(text.strip())

def scrape_cur_tradingview(cur) -> Dict[str, Any]:
    tv_symbol = cur.get("tradingview")
    symbol = cur.get("symbol")
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
        url = SOURCES["tradingview"].format(symbol=tv_symbol)
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        # Giá
        price_element = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//span[contains(@class, 'js-symbol-last')]//span[1]")
            )
        )
        price = parse_number(price_element.text)
        # Đơn vị
        currency_element = driver.find_element(
            By.XPATH, "//span[contains(@class, 'js-symbol-currency')]"
        )
        currency = currency_element.text
        # Thay đổi tuyệt đối
        change_element = driver.find_element(
            By.XPATH, "//div[contains(@class, 'change-zoF9r75I')]//span[1]"
        )
        change = parse_number(change_element.text)
        # Thay đổi phần trăm
        change_percent_element = driver.find_element(
            By.XPATH, "//span[contains(@class, 'js-symbol-change-pt')]"
        )
        change_percent = parse_number(
            change_percent_element.text.replace("%", "").replace("+", "").replace("−", "-")
        )
        # Thời gian cập nhật TradingView
        try:
            time_element = driver.find_element(
                By.XPATH, "//span[contains(@class, 'js-symbol-lp-time')]"
            )
            last_update = time_element.text
        except Exception:
            last_update = None

        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "unit": "VND",
            "currency": currency,
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "last_update": last_update,
            "source": "tradingview",
            "data_type": "real-time"
        }

        logger.info(f"{symbol} TradingView: {price} {change:+.2f} ({change_percent:+.2f}%)")
        return data
    except Exception as e:
        logger.error(f"Error scraping TradingView {symbol}: {e}")
        return None
    finally:
        driver.quit()

def scrape_cur_investing(cur) -> Dict[str, Any]:
    symbol = cur.get("symbol")
    inv_symbol = cur.get("investing")
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
        url = SOURCES["investing"].format(slug=inv_symbol)
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        price_element = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[data-test='instrument-price-last']")
            )
        )
        price = parse_number(price_element.text)

        change_element = driver.find_element(
            By.CSS_SELECTOR,
            "[data-test='instrument-price-change']"
        )
        change = parse_number(change_element.text)
        
        # Change percent
        change_percent_element = driver.find_element(
            By.CSS_SELECTOR,
            "[data-test='instrument-price-change-percent']"
        )
        change_percent_text = change_percent_element.text
        change_percent = parse_number(
            change_percent_text.replace("(", "").replace(")", "").replace("%", "")
        )
        
        last_update = None
        
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "unit": "VND",
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "last_update": last_update,
            "source": "investing.com",
            "data_type": "real-time"
        }
        
        logger.info(f"{symbol} data scraped: {price} {change:+.2f} ({change_percent:+.2f}%)")
        return data
        
    except Exception as e:
        logger.error(f"Error scraping Investing.com {symbol}: {e}")
        return None
    finally:
        driver.quit()

def get_currency_data(cur, source: str = "investing") -> Dict[str, Any]:
    source_priority = []
    s = source.lower()
    if s in ["tradingview", "trading view"]:
        source_priority = ["tradingview", "investing"]
    else:
        source_priority = ["investing", "tradingview"]

    for src in source_priority:
        try: 
            if src == "tradingview":
                logger.info(f"Thử lấy dữ liệu từ TradingView cho {cur['symbol']}...")
                data = scrape_cur_tradingview(cur)
            else:
                logger.info(f"Thử lấy dữ liệu từ Investing.com cho {cur['symbol']}...")
                data = scrape_cur_investing(cur)
            if data:
                logger.info(f"Thành công với nguồn: {src}")
                return data
        except Exception as e:
            logger.warning(f"Lỗi với nguồn {src} cho {cur['symbol']}: {e}")
            continue
    logger.error(f"Tất cả các nguồn đều thất bại cho {cur['symbol']}!")
    return None

if __name__ == "__main__":
    results = {}
    for cur in cur_list:
        data = get_currency_data(cur, source="tradingview")
        results[cur["symbol"]] = data
    print(json.dumps(results, indent=2, ensure_ascii=False))