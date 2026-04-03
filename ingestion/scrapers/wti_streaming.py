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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCES = {
    "tradingview": "https://www.tradingview.com/symbols/WTI/",
    "vn_investing": "https://vn.investing.com/commodities/crude-oil"
}


def parse_number(text: str) -> float:
    if not text:
        return 0.0
    return float(text.replace(",", "").strip())


def scrape_wti_tradingview() -> Dict[str, Any]:
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
        driver.get(SOURCES["tradingview"])
        wait = WebDriverWait(driver, 15)
        price_element = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//span[contains(@class, 'header-subtitle')]")
            )
        )
        # tách giá 
        price = parse_number(price_element.text)
        
        # Extract change
        change_element = driver.find_element(
            By.XPATH,
            "//span[contains(@data-test-id, 'change')]"
        )
        change_text = change_element.text 
        change = parse_number(change_text)
        
        # Extract change %
        change_percent_element = driver.find_element(
            By.XPATH,
            "//span[contains(@data-test-id, 'change-percent')]"
        )
        change_percent_text = change_percent_element.text 
        change_percent = parse_number(
            change_percent_text.replace("(", "").replace(")", "").replace("%", "")
        )
        
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": "WTI",
            "currency": "USD",
            "unit": "USD/barrel",
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "source": "tradingview",
            "data_type": "real-time"
        }
        
        logger.info(f"WTI data scraped: ${price} {change:+.2f} ({change_percent:+.2f}%)")
        return data
        
    except Exception as e:
        logger.error(f"Error scraping TradingView WTI: {e}")
        return None
    finally:
        driver.quit()


def scrape_wti_investing() -> Dict[str, Any]:
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
        driver.get(SOURCES["vn_investing"])
        wait = WebDriverWait(driver, 15)
        
        # Using data-test attributes from your HTML
        price_element = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[data-test='instrument-price-last']")
            )
        )
        price = parse_number(price_element.text)
        
        # Change value
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
        
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": "WTI",
            "currency": "USD",
            "unit": "USD/barrel",
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "source": "tradingview",
            "data_type": "real-time"
        }
        
        logger.info(f"WTI data scraped: ${price} {change:+.2f} ({change_percent:+.2f}%)")
        return data
        
    except Exception as e:
        logger.error(f"Error scraping Investing.com WTI: {e}")
        return None
    finally:
        driver.quit()


def get_wti_data(source: str = "investing") -> Dict[str, Any]:
    source_priority = []
    s = source.lower()
    if s in ["tradingview", "trading view"]:
        source_priority = ["tradingview", "investing"]
    else:
        source_priority = ["investing", "tradingview"]

    for src in source_priority:
        try:
            if src == "tradingview":
                logger.info("Thử lấy dữ liệu từ TradingView...")
                data = scrape_wti_tradingview()
            else:
                logger.info("Thử lấy dữ liệu từ Investing.com...")
                data = scrape_wti_investing()
            if data:
                logger.info(f"Thành công với nguồn: {src}")
                return data
        except Exception as e:
            logger.warning(f"Lỗi với nguồn {src}: {e}")
            continue
    logger.error("Tất cả các nguồn đều thất bại!")
    return None


if __name__ == "__main__":
    data = get_wti_data(source="investing")
    if data:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print("Failed to scrape WTI data")
