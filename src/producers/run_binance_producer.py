import requests
import json
import time
from datetime import datetime
from confluent_kafka import Producer

# Kafka Configuration
producer = Producer({'bootstrap.servers': 'localhost:9092'})
KAFKA_TOPIC = 'crypto-trends'
SYMBOLS = ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]

def fetch_latest_sentiment(symbol):
    """Fetches the most recent Long/Short ratio data point."""
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {"symbol": symbol, "period": "5m", "limit": 1}
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if data:
            record = data[0]
            dt = datetime.now() # Real-time arrival timestamp

            if symbol == "BTCUSDT":
                crypto_name = "bitcoin"
            elif symbol == "ETHUSDT":
                crypto_name = "ethereum"
            elif symbol == "DOGEUSDT":
                crypto_name = "dogecoin"
            else:
                crypto_name = symbol.replace("USDT", "").lower()

            return {
                "currency": crypto_name,
                "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "long_short_ratio": float(record['longShortRatio']),
                "long_account_pct": float(record['longAccount']),
                "short_account_pct": float(record['shortAccount'])
            }
    except Exception as e:
        print(f"API Error for {symbol}: {e}")
    return None

if __name__ == "__main__":
    print("Starting Real-Time Binance Sentiment Producer...")
    try:
        while True:
            for s in SYMBOLS:
                latest_data = fetch_latest_sentiment(s)
                if latest_data:
                    producer.produce(KAFKA_TOPIC, json.dumps(latest_data))
                    producer.flush()
                    print(f"Sent to Kafka: {latest_data}")
            
            # Wait 60 seconds for the next update
            time.sleep(60)
    except KeyboardInterrupt:
        print("Producer stopped.")