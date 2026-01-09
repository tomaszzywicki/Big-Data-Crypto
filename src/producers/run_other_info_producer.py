import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError
from pathlib import Path
from ..kafka_utils import read_config, create_topic
from ..scrapers.price import fetch_other_info

CONFIG_PATH = str(Path(__file__).parent.parent.parent / "config" / "kafka.properties")
TOPIC = "crypto-other-info"
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

    print("Starting scraping additional info...")

    try:
        while True:
            for coin in COINS:
                # 1. Pobranie danych
                info = fetch_other_info(coin)

                now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                info["timestamp"] = now

                # 2. Serializacja
                info_json = json.dumps(info)

                # 3. Klucz do kafki
                key_bytes = coin.encode(encoding="utf-8")

                producer.produce(topic=TOPIC, value=info_json, key=key_bytes, on_delivery=delivery_report)

            # producer.pool(0)

            time.sleep(3)

    except Exception as e:
        print(f"[!] SOmething went wrong: {e}")

    finally:
        producer.flush()
        print(f"{delivered_records} messages were produced to topic {TOPIC}.")


if __name__ == "__main__":
    main()
