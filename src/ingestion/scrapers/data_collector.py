from src.scrapers.scraper import CoinMarketCapScraper
from datetime import datetime
from selenium.webdriver.common.by import By
import time
from pathlib import Path
from typing import Set
from selenium.webdriver.remote.webelement import WebElement


class CoinMarketCapDataCollector:

    REFRESH_INTERNVAL = 100
    PRICE_DELAY = 10
    POST_DELAY = 10

    def __init__(self, crypto_name: str, url: str):
        self.crypto_name = crypto_name
        self.url = url

    def _get_timestamp(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%s")
        return timestamp

    def collect_prices(self):
        scraper = CoinMarketCapScraper(self.url)
        iteration = 0

        while True:
            try:
                # refreshing page after some time
                if iteration % self.REFRESH_INTERNVAL == 0 and iteration > 0:
                    scraper.driver.refresh()
                    time.sleep(self.PRICE_DELAY)

                price = scraper.get_crypto_price(delay=self.PRICE_DELAY)
                timestamp = self._get_timestamp()

                print(f"{self.crypto_name:10s}: {price:15s} at {timestamp}")

                iteration += 1

            except Exception as e:
                print(f"Error collecting price for {self.crypto_name}: {e}")
                time.sleep(30)

    def collect_additional_info(self):
        """
        Volume, FDV, Total supply, Max supply etc
        """
        scraper = CoinMarketCapScraper(self.url)
        iteration = 0

        while True:
            try:
                if iteration % self.REFRESH_INTERNVAL == 0 and iteration > 0:
                    scraper.driver.refresh()
                    time.sleep(self.PRICE_DELAY)

                data = scraper.get_additional_info(delay=self.PRICE_DELAY)
                timestamp = self._get_timestamp()

                print(f"{self.crypto_name:10s} {data} at {timestamp}")

                iteration += 1

            except Exception as e:
                print(f"Error collecting additional info for {self.crypto_name}: {e}")
                time.sleep(30)

    def collect_posts(self):
        scraper = CoinMarketCapScraper(self.url)
        seen_posts: Set[str] = set()

        while True:
            try:
                posts_div = scraper.get_last_posts(delay=self.POST_DELAY)
                posts = posts_div.find_elements(By.CSS_SELECTOR, '[data-test="community-post"]')

                new_posts = self._process_posts(posts, seen_posts)
                print(f"Found {len(posts)} posts for {self.crypto_name} ({new_posts} new)")

                time.sleep(60)

            except Exception as e:
                print(f"Error collecting posts for {self.crypto_name}: {e}")
                time.sleep(60)

    def _process_posts(self, posts: list[WebElement], seen_posts: Set[str]) -> int:
        new_count = 0
        filepath = f"{self.crypto_name}_posts.txt"

        with open(filepath, "a", encoding="utf-8") as file:
            for post in posts:
                post_id = post.get_attribute("data-post-id")

                if post_id in seen_posts:
                    continue

                if post_id:
                    seen_posts.add(post_id)
                    new_count += 1

                try:
                    text_element = post.find_element(By.CLASS_NAME, "text")
                    full_text = text_element.get_attribute("textContent")

                    if full_text:
                        file.write(f"{post_id};{full_text}\n")
                except Exception as e:
                    print(f"Error processing post {post_id}: {e}")

        return new_count
