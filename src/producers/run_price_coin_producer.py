import time
import json
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Producer
from ..kafka_utils import create_topic, read_config
from ..ingestion.scrapers.scraper import CoinMarketCapScraper

# Konfiguracja
CONFIG_PATH = str(Path(__file__).parent.parent.parent / "config" / "kafka.properties")
TOPIC = "crypto-prices"
# Mapowanie nazw na URL-e
CRYPTO_URLS = {
    "bitcoin": "https://coinmarketcap.com/currencies/bitcoin/",
    "ethereum": "https://coinmarketcap.com/currencies/ethereum/",
    "dogecoin": "https://coinmarketcap.com/currencies/dogecoin/",
}

delivered_records = 0

def delivery_report(err, msg):
    global delivered_records
    if err is not None:
        print(f"[!] Błąd dostarczenia: {err}", flush=True)
    else:
        delivered_records += 1
        print(f"[+] Przesłano {msg.key().decode('utf-8')} do partycji {msg.partition()} @ offset {msg.offset()}", flush=True)

def clean_price(price_str):
    """Zamienia '$97,241.38' na float 97241.38"""
    try:
        return float(price_str.replace('$', '').replace(',', ''))
    except Exception as e:
        print(f"Błąd konwersji ceny '{price_str}': {e}")
        return None

def main():
    # 1. Inicjalizacja Kafki
    conf = read_config(CONFIG_PATH)
    create_topic(conf, TOPIC, num_partitions=3, replication_factor=1)
    producer = Producer(conf)

    # 2. Inicjalizacja scraperów (jeden dla każdej waluty, aby nie przełączać URL w kółko)
    print("Inicjalizacja przeglądarek dla walut...")
    scrapers = {name: CoinMarketCapScraper(url) for name, url in CRYPTO_URLS.items()}

    print("Zaczynamy przesyłanie rzeczywistych danych do Kafki...")

    try:
        while True:
            for name, scraper in scrapers.items():
                try:
                    # Pobranie surowej ceny ze strony
                    raw_price = scraper.get_crypto_price(delay=2)
                    price = clean_price(raw_price)

                    if price is not None:
                        # Przygotowanie danych
                        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        data = {
                            "currency": name,
                            "price": price,
                            "timestamp": now
                        }

                        # Serializacja i wysyłka
                        value_json = json.dumps(data)
                        key_bytes = name.encode("utf-8")

                        producer.produce(
                            topic=TOPIC,
                            value=value_json,
                            key=key_bytes,
                            on_delivery=delivery_report
                        )
                
                except Exception as e:
                    print(f"Błąd podczas scrapowania {name}: {e}")

            # Callbacki i oczekiwanie
            producer.poll(0)
            # CoinMarketCap nie lubi zbyt częstych odświeżeń, 10s jest bezpieczniejsze
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nZatrzymywanie producenta...")
    finally:
        producer.flush()
        print(f"Koniec. Przesłano łącznie {delivered_records} wiadomości.")

if __name__ == "__main__":
    main()