import requests
import time
from datetime import datetime

def fetch_binance_sentiment(symbol="BTCUSDT"):
    # Endpoint Binance Futures for Long/Short Ratio 
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {
        "symbol": symbol,
        "period": "5m",
        "limit": 1      
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data:
            latest = data[0]
            return {
                "coin_id": symbol.replace("USDT", "").lower(),
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "long_short_ratio": float(latest['longShortRatio']),
                "long_account_pct": float(latest['longAccount']),
                "short_account_pct": float(latest['shortAccount'])
            }
    except Exception as e:
        return f"Error for {symbol}: {e}"

if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT"]
    print(f"--- TESTING LIVE SENTIMENT (Interval: 60s, 10 iterations) ---")
    
    try:
        for i in range(10):
            print(f"\n[Read {i+1}/10] {datetime.now().strftime('%H:%M:%S')}")
            
            for s in symbols:
                res = fetch_binance_sentiment(s)
                if isinstance(res, dict):
                    print(f"  {res['coin_id'].upper()}: Ratio: {res['long_short_ratio']} | Longs: {res['long_account_pct']}% | Shorts: {res['short_account_pct']}%")
                else:
                    print(f"  Error: {res}")
            
            if i < 9:
                time.sleep(60) 
    except KeyboardInterrupt:
        print("\nInterrupted.")