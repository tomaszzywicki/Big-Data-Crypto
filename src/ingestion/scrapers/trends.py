import requests
from datetime import datetime

def fetch_sentiment(coin_id):
    """
    Fetches timestamp and community sentiment (up/down votes) 
    for a specific cryptocurrency.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        'localization': 'false',
        'tickers': 'false',
        'market_data': 'false',
        'community_data': 'false',
        'developer_data': 'false',
        'sparkline': 'false'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Mapping the sentiment data
        return {
            "coin_id": coin_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sentiment_up": data.get('sentiment_votes_up_percentage', 0.0),
            "sentiment_down": data.get('sentiment_votes_down_percentage', 0.0)
        }
    except Exception as e:
        print(f"Error fetching sentiment for {coin_id}: {e}")
        return None