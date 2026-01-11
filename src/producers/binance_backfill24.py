import requests
import json
from datetime import datetime
from confluent_kafka import Producer

# Kafka Configuration
kafka_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_config)
KAFKA_TOPIC = 'crypto-trends'

def backfill_binance_history(symbol="BTCUSDT"):
    """Fetches the last 24h of Long/Short ratio data (5m intervals)."""
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {
        "symbol": symbol,
        "period": "5m",  # 5-minute intervals
        "limit": 288     # 288 * 5min = 24 hours
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print(f"Fetching {len(data)} historical records for {symbol}...")
        
        for record in data:
            # Convert Binance timestamp (ms) to Hive-friendly format
            dt = datetime.fromtimestamp(record['timestamp'] / 1000.0)
            
            payload = {
                "coin_id": symbol.replace("USDT", "").lower(),
                "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "long_short_ratio": float(record['longShortRatio']),
                "long_account_pct": float(record['longAccount']),
                "short_account_pct": float(record['shortAccount'])
            }
            
            # Produce to Kafka
            producer.produce(KAFKA_TOPIC, json.dumps(payload))
        
        producer.flush()
        print(f"Successfully backfilled {symbol} into the pipeline.")
        
    except Exception as e:
        print(f"Error during backfill for {symbol}: {e}")

if __name__ == "__main__":
    print("Starting Historical Data Backfill...")
    for s in ["BTCUSDT", "ETHUSDT"]:
        backfill_binance_history(s)
    print("Backfill complete.")