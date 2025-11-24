import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
