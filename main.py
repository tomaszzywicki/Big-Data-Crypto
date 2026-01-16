from multiprocessing import Process
from src.ingestion.scrapers.data_collector import CoinMarketCapDataCollector


def run_collector(crypto_name: str, url: str, collect_type: str):
    collector = CoinMarketCapDataCollector(crypto_name, url)

    if collect_type == "prices":
        collector.collect_prices()
    elif collect_type == "posts":
        collector.collect_posts()
    elif collect_type == "additional_info":
        collector.collect_additional_info()
    else:
        raise Exception("Invalid collect type")


def main():
    cryptos = {
        "bitcoin": "https://coinmarketcap.com/currencies/bitcoin/",
        "ethereum": "https://coinmarketcap.com/currencies/ethereum/",
        "dogecoin": "https://coinmarketcap.com/currencies/dogecoin/",
    }

    print("Starting cryptocurrency data collectors...")

    processes = []

    for name, url in cryptos.items():
        p1 = Process(target=run_collector, args=(name, url, "prices"))
        p1.start()
        processes.append(p1)

        # p2 = Process(target=run_collector, args=(name, url, "posts"))
        # p2.start()
        # processes.append(p2)

        # p3 = Process(target=run_collector, args=(name, url, "additional_info"))
        # p3.start()
        # processes.append(p3)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
