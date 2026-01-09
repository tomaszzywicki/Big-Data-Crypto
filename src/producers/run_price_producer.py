import time
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError
import json
from pathlib import Path
from ..kafka_utils import create_topic, read_config

from ..ingestion.scrapers.price import fetch_price


CONFIG_PATH = str(Path(__file__).parent.parent.parent / "config" / "kafka.properties")
TOPIC = "crypto-prices"
COINS = ["bitcoin", "ethereum", "dogecoin"]

delivered_records = 0


def delivery_report(err, msg):
    global delivered_records

    if err is not None:
        print(f"[!] Failed to deliver message: {err}", flush=True)
    else:
        delivered_records += 1
        print(
            f"[+] Produced record to topic {TOPIC} partition {msg.partition()} @ offset {msg.offset()}",
            flush=True,
        )


def main():
    conf = read_config(CONFIG_PATH)

    create_topic(conf, TOPIC, num_partitions=3, replication_factor=1)

    producer = Producer(conf)

    print("Zaczynamy scrapowanko...")

    last_prices = {coin: None for coin in COINS}
    try:
        while True:
            for coin in COINS:
                # 1. Pobieramy dane
                data = fetch_price(coin, last_prices[coin])

                last_price = data["price"]

                now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                data["timestamp"] = now

                # 2. Serializacja
                value_json = json.dumps(data)

                # 3. Klucz (potrzebny do wstawiania przez kafkę do odpowiednich partycji)
                key_bytes = coin.encode("utf-8")

                # 4. Wysyłamy
                producer.produce(
                    topic=TOPIC,
                    value=value_json,
                    key=key_bytes,
                    on_delivery=delivery_report,
                    headers=None,
                )

            # Obsługujemy callbacki
            producer.poll(0)

            # Czekamy przed kolejnym fetchem
            time.sleep(5)

    except KeyboardInterrupt:
        print("Zatrzymujemy...")
    except Exception as e:
        print(f"[!] Something went wrong when producing messages: {e}")
    finally:
        producer.flush()
        print(f"{delivered_records} messages were produced to topic {TOPIC}.")


if __name__ == "__main__":
    main()
