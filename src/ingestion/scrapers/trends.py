import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime
import time

def fetch_trends(keywords: list) -> dict:
    """
    Fetches the current 'Interest over time' index (0-100) for given keywords.
    Returns a dictionary: {'timestamp': 'YYYY-MM-DD HH:MM:SS', 'bitcoin': 80, ...}
    """
    try:
        # 1. Connect to Google Trends
        # hl='en-US': Host language (English)
        # tz=0: Timezone Offset (0 means UTC)
        # timeout: Tuple (connect timeout, read timeout) to avoid hanging
        pytrends = TrendReq(hl='en-US', tz=0, timeout=(10, 25))
        
        # 2. Build the payload (prepare the query)
        # timeframe='now 1-H': Get data for the last 1 hour (minute-by-minute granularity)
        # cat=0: All categories
        pytrends.build_payload(keywords, cat=0, timeframe='now 1-H', geo='', gprop='')

        # 3. Fetch the data (Interest Over Time)
        data = pytrends.interest_over_time()

        if data.empty:
            print("Warning: Google Trends returned empty data.")
            return {}

        # 4. Extract the latest data point
        # We only care about the most recent minute (the last row in the DataFrame)
        latest_data = data.iloc[-1]
        
        # 5. Format the result
        result = {}
        
        # Add a timestamp string (easier for serialization/Kafka later)
        result['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for kw in keywords:
            # Check if keyword exists in response (Google sometimes changes column names)
            if kw in latest_data:
                # Cast to native Python int (numpy types can cause issues with JSON serialization)
                result[kw] = int(latest_data[kw])
        
        return result

    except Exception as e:
        print(f"Error fetching Google Trends: {e}")
        return {}
    
def fetch_trends_last24(keywords: list) -> list:
    """
    Fetches Google Trends data for the last 24 hours.
    Smallest possible interval for 24h is typically 8 minutes.
    Returns a list of dictionaries (one for each time point).
    """
    try:
        # Initialize pytrends
        pytrends = TrendReq(hl='en-US', tz=0, timeout=(10, 25))
        
        # 'now 1-d' covers the last 24 hours
        pytrends.build_payload(keywords, cat=0, timeframe='now 1-d', geo='', gprop='')

        # Get the full history for this timeframe
        df = pytrends.interest_over_time()

        if df.empty:
            print("Warning: No data found for the last 24h.")
            return []

        # Remove the 'isPartial' column if it exists (internal Google metadata)
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])

        # Transform DataFrame to a list of dictionaries for Kafka/JSON
        # Each dict will look like: {'timestamp': '...', 'bitcoin': 50, 'BTC': 20, ...}
        results = []
        for timestamp, row in df.iterrows():
            record = {'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S")}
            for kw in keywords:
                record[kw] = int(row[kw])
            results.append(record)
            
        return results

    except Exception as e:
        print(f"Error fetching 24h trends: {e}")
        return []

if __name__ == "__main__":
    test_keywords = ["bitcoin", "ethereum", "crypto"]
    # print(f"Testing fetch for: {test_keywords}...")
    
    # start_time = time.time()
    # output = fetch_trends(test_keywords)
    # duration = time.time() - start_time
    
    # print(f"Execution time: {duration:.2f}s")
    # print("Result received:")
    # print(output)

    # KEYWORDS = ["bitcoin", "BTC", "buy bitcoin", "sell bitcoin", "bitcoin crash",
    #         "ethereum", "ETH", "buy ethereum", "sell ethereum", "ethereum crash",
    #         "crypto", "cryptocurrency", "buy crypto", "sell crypto", "crypto crash"]
    
    print(f"Testing fetch for last 24h: {test_keywords}...")

    history = fetch_trends_last24(test_keywords)

    for record in history:
        print(record)

    print(f"Received {len(history)} data points.")
    if history:
        print("First point:", history[0])
        print("Last point: ", history[-1])