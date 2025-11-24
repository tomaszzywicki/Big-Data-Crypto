from scraper import CoinMarketCapScraper
from multiprocessing import Process
from datetime import datetime
from selenium.webdriver.common.by import By
import os
import time

urls = {
    "btc": "https://coinmarketcap.com/currencies/bitcoin/",
    "eth": "https://coinmarketcap.com/currencies/ethereum/",
    "xrp": "https://coinmarketcap.com/currencies/xrp/",
}

filenames = ["btc.txt", "eth.txt", "xrp.txt", "bnb.txt", "sol.txt", "doge.txt"]


def save_prices(url: str, filepath: str, name: str):
    scraper = CoinMarketCapScraper(url)
    iteration = 0

    while True:
        if iteration % 100 == 0 and iteration > 0:
            print(f"Refreshing page for {name}")
            scraper.driver.refresh()
            time.sleep(10)

        day = datetime.now().strftime("%Y-%m-%d")
        with open(f"data/{filepath}_prices_{day}", "a+", encoding="utf-8") as file:
            current_time = datetime.now().strftime("%H:%M:%S %Y-%m-%d")
            price = scraper.get_crypto_price(delay=10)
            print(f"{name:10s}: {price:15s} at {current_time}")
            file.write(f"{price};{current_time}\n")
            file.flush()


def save_posts(url: str, filepath: str, name: str):
    scraper = CoinMarketCapScraper(url)
    seen_posts = set()

    while True:
        day = datetime.now().strftime("%Y-%m-%d")
        with open(f"data/{filepath}_posts_{day}.txt", "a+", encoding="utf-8") as file:
            posts_div = scraper.get_last_posts(delay=10)

            # this is list of divs containing one whole post
            posts = posts_div.find_elements(By.CSS_SELECTOR, '[data-test="community-post"]')

            print(f"Found {len(posts)} posts for {name}.")
            for post in posts:
                post_id = post.get_attribute("data-post-id")
                if post_id in seen_posts:
                    continue

                seen_posts.add(post_id)

                text = post.find_element(By.CLASS_NAME, "text")
                full_text = text.get_attribute("textContent")
                if full_text:
                    file.write(f"{post_id};{full_text}\n\n")


def main():
    if not os.path.isdir("./data"):
        print("Creating data directory")
        os.mkdir("./data")

    crypto = ["btc", "eth", "xrp"]
    for c in crypto:
        if not os.path.isdir(f"./data/{c}"):
            print(f"Creating /data{c} directory")
            os.mkdir(f"./data/{c}")

    print("Starting scrapers 🟩")

    p0 = Process(target=save_prices, args=(urls["btc"], "btc", "Bitcoin"))
    p1 = Process(target=save_prices, args=(urls["eth"], "eth", "Ethereum"))
    p2 = Process(target=save_posts, args=(urls["btc"], "btc_posts.txt", "Bitcoin"))
    p3 = Process(target=save_posts, args=(urls["eth"], "eth_posts.txt", "Ethereum"))

    p0.start()
    p1.start()
    p2.start()
    p3.start()

    p0.join()
    p1.join()
    p2.join()
    p3.join()


if __name__ == "__main__":
    main()
