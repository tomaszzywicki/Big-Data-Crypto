import time
import json
import sys
import os
from pathlib import Path
from confluent_kafka import Producer

# Add the parent directory to sys.path to allow imports from 'src' module
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from src.kafka_utils import create_topic, read_config
    from src.ingestion.scrapers.trends import fetch_trends
except ImportError as e:
    print("Import Error! Run this script as a module from the project root.")
    print(f"Details: {e}")
    sys.exit(1)

# --- CONFIGURATION ---
# Path to Kafka configuration file (kafka.properties)
CONFIG_PATH = str(Path(__file__).parent.parent.parent / "config" / "kafka.properties")
TOPIC = "crypto-trends"
# Updated keywords for better analysis
KEYWORDS = ["bitcoin", "BTC", "buy bitcoin", "sell bitcoin", "bitcoin crash",
            "ethereum", "ETH", "buy ethereum", "sell ethereum", "ethereum crash",
            "crypto", "cryptocurrency", "buy crypto", "sell crypto", "crypto crash"]

def delivery_report(err, msg):
    """
    Callback called once the message is successfully delivered (or failed).
    """
    if err is not None:
        print(f"[!] Delivery failed: {err}")
    else:
        print(f"[+] Message sent: {msg.value().decode('utf-8')} to topic: {msg.topic()}")

def main():
    print(f"--- Google Trends Producer Started ---")
    print(f"Target Topic: {TOPIC}")
    print(f"Tracking Keywords: {KEYWORDS}")

    # 1. Load Config & Create Topic
    if not os.path.exists(CONFIG_PATH):
        print(f"[!] Config file not found at: {CONFIG_PATH}")
        return

    conf = read_config(CONFIG_PATH)
    # Create topic with 1 partition and replication factor 1 (simple setup)
    create_topic(conf, TOPIC, num_partitions=1, replication_factor=1)

    producer = Producer(conf)

    try:
        while True:
            # 2. Fetch Data (using your module from Step 1)
            print(f"Fetching data from Google Trends...")
            data = fetch_trends(KEYWORDS)

            if data:
                # 3. Serialize to JSON
                value_json = json.dumps(data)

                # 4. Send to Kafka
                # We use 'bitcoin' as the key to ensure ordering if we had multiple partitions
                key_bytes = "trends".encode("utf-8")
                
                producer.produce(
                    topic=TOPIC,
                    value=value_json,
                    key=key_bytes,
                    on_delivery=delivery_report
                )
                
                # Handle callbacks (essential for delivery reports)
                producer.poll(0)
            else:
                print("[!] Received empty data from Google Trends.")

            # 5. Wait before next fetch
            # Google Trends rate limits are strict. 60 seconds is a safe starting point.
            print("Waiting 60 seconds...")
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    except Exception as e:
        print(f"[!] Unexpected error: {e}")
    finally:
        # Ensure all messages are sent before exiting
        print("Flushing producer...")
        producer.flush()

if __name__ == "__main__":
    main()