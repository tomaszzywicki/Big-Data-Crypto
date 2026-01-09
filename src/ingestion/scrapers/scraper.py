import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import requests


class CoinMarketCapScraper:
    def __init__(self, url: str) -> None:
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--headless")  # No UI
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--window-size=1920,1080")  # Full screen żeby było widać wszystie elementy
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)

        self.driver = webdriver.Chrome(options=self.options)
        self.driver.get(url)

    def get_crypto_price(self, delay: int = 5) -> str:
        time.sleep(delay)  # waiting between js refreshes the price
        price_element = self.driver.find_element(By.CSS_SELECTOR, '[data-test="text-cdp-price-display"]')
        return price_element.text

    def get_last_posts(self, delay: int = 10):
        # refresh site to get new posts
        self.driver.refresh()
        wait = WebDriverWait(self.driver, delay)

        # get 'latest' button and click it
        element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-index="tab-Latest"]')))
        element.click()
        # to make sure posts are loaded
        time.sleep(5)

        posts_div = self.driver.find_element(By.CSS_SELECTOR, '[data-test="coin-community-post-list"]')

        return posts_div

    def get_additional_info(self, delay: int = 5):
        self.driver.refresh()
        time.sleep(delay)
        wait = WebDriverWait(self.driver, 10)
        result = {}

        try:
            volume_icon = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-test="icon-cex-dex-volume"]'))
            )

            actions = ActionChains(self.driver)
            actions.move_to_element(volume_icon).perform()

            time.sleep(1)

            volume_elements = self.driver.find_elements(By.CSS_SELECTOR, ".htpYOz")
            if len(volume_elements) == 2:
                result["cex_volume_24h"] = volume_elements[0].text
                result["dex_volume_24h"] = volume_elements[1].text
            elif len(volume_elements) == 1:
                result["cex_volume_24h"] = volume_elements[0].text
                result["dex_volume_24h"] = None
            else:
                result["cex_volume_24h"] = None
                result["dex_volume_24h"] = None

            actions.move_by_offset(300, 300).perform()

        except Exception as e:
            print(f"Error getting volume: {e}")
            result["volume_24h"] = None

        return result
