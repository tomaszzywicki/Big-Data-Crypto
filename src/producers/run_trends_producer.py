import json
import time
from confluent_kafka import Producer
from src.ingestion.scrapers.trends import fetch_sentiment

# Kafka Setup
p = Producer({'bootstrap.servers': 'localhost:9092'})
COINS = ['bitcoin', 'ethereum']

if __name__ == "__main__":
    print(f"Starting Multi-Coin Sentiment Producer (Interval: 3m)...")
    
    try:
        while True:
            for coin in COINS:
                data = fetch_sentiment(coin)
                if data:
                    p.produce('crypto-trends', json.dumps(data))
                    p.flush()
                    print(f"Sent to Kafka: {data}")
            
            print("--- Cycle complete. Waiting 3 minutes... ---")
            time.sleep(180) # 3 minutes wait
            
    except KeyboardInterrupt:
        print("Stopped by user.")