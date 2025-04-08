import requests
import time
import pymongo
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient

# MongoDB Connection
MONGODB_URI = "mongodb://localhost:27017/bitget10K"
client = MongoClient(MONGODB_URI)
db = client["bitget10K"]

# Bitget API Endpoint for Klines
BITGET_URL = "https://api.bitget.com/api/v2/spot/market/history-candles"

# Parameters
TOKENS = ["ETHUSDT", "ADAUSDT"]
INTERVAL = "1min"
LIMIT = 200  # Max per request
TOTAL_RECORDS = 10000  # Maintain only the latest XXXX records per token
SLEEP_TIME = 1  # Sleep to avoid rate limits

def create_time_series_collection():
    """Create time series collections for each token if they don't exist."""
    for token in TOKENS:
        collection_name = f"{token}_timeseries"
        if collection_name not in db.list_collection_names():
            db.create_collection(
                collection_name,
                timeseries={"timeField": "timestamp", "metaField": "token", "granularity": "minutes"}
            )
            print(f"Created time series collection: {collection_name}")

def fetch_price_data(symbol, end_time, limit):
    """Fetch historical price data within given end time"""
    params = {
        "symbol": symbol,
        "granularity": INTERVAL,
        "endTime": end_time,
        "limit": limit
    }
    try:
        response = requests.get(BITGET_URL, params=params)
        response.raise_for_status()
        data = response.json()["data"]
        return data if isinstance(data, list) else []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return []

def save_to_mongodb(collection, data, token):
    """Save all items in response to MongoDB and ensure size limit."""
    if not data:
        return
    
    records = [
        {
            "token": token,
            "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, timezone.utc),
            "open": float(entry[1]),
            "high": float(entry[2]),
            "low": float(entry[3]),
            "close": float(entry[4]),
            "base_volume": float(entry[5]),
            "quote_volume": float(entry[6]),
        }
        for entry in data
    ]
    
    if records:
        # Avoid duplicates
        existing_timestamps = {doc["timestamp"] for doc in collection.find({"token": token}, {"timestamp": 1})}
        filtered_records = [r for r in records if r["timestamp"] not in existing_timestamps]
        
        if filtered_records:
            collection.insert_many(filtered_records)
            print(f"Saved {len(filtered_records)} new records to {collection.name}")
            delete_oldest_records(collection)
        else:
            print("No new records to save.")

def delete_oldest_records(collection):
    """Ensure only the latest TOTAL_RECORDS are kept."""
    record_count = collection.count_documents({})
    if record_count > TOTAL_RECORDS:
        excess = record_count - TOTAL_RECORDS
        oldest_records = collection.find().sort("timestamp", pymongo.ASCENDING).limit(excess)
        ids_to_delete = [record["_id"] for record in oldest_records]
        collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(f"Deleted {excess} oldest records from {collection.name}")

def fetch_initial_data():
    """Fetch initial TOTAL_RECORDS per token."""
    for token in TOKENS:
        collection = db[f"{token}_timeseries"]
        if collection.count_documents({}) >= TOTAL_RECORDS:
            print(f"Database already has {TOTAL_RECORDS} records for {token}. Skipping initial fetch.")
            continue
        
        end_time = int(time.time() * 1000)
        fetched = 0
        retries = 0
        
        while fetched < TOTAL_RECORDS and retries < 5:
            price_data = fetch_price_data(token, end_time, LIMIT)
            if not price_data:
                print(f"No data returned for {token}, retrying...")
                retries += 1
                time.sleep(5)
                continue
            
            save_to_mongodb(collection, price_data, token)
            fetched += len(price_data)
            if price_data:
                end_time = int(price_data[-1][0]) - 60000 * 199# Corrected line
                print(f"end_time here is {end_time}")
            else:
                break
            print(f"Total records saved for {token}: {fetched}/{TOTAL_RECORDS}")
            retries = 0  # Reset retries after successful fetch
            time.sleep(SLEEP_TIME)
        
        if fetched >= TOTAL_RECORDS:
            print(f"Successfully fetched {TOTAL_RECORDS} records for {token}.")
        else:
            print(f"Stopped fetching for {token} after {retries} retries. Total fetched: {fetched}")

def fill_gaps():
    """Fill missing minute gaps from last record to now."""
    for token in TOKENS:
        collection = db[f"{token}_timeseries"]
        last_record = collection.find_one(sort=[("timestamp", -1)])
        print(f"last_record {last_record}")
        if not last_record:
            print(f"No records found for {token}. Skipping gap fill.")
            continue
        
        last_ts = last_record["timestamp"]
        
        gap_start = int(last_ts.replace(tzinfo=timezone.utc).timestamp() * 1000) + 60000
        
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        now_ms = int(now.timestamp() * 1000)

        if gap_start > now_ms:
            print(f"No gaps to fill for {token}.")
            continue
        
        total_minutes = (now_ms - gap_start) // 60000 - 1
        print(f"total mins {total_minutes}")
        
        fetched = 0
        end_time_ms = now_ms
        
        while fetched < total_minutes & total_minutes > 0:
            remaining = total_minutes - fetched
            limit = min(LIMIT, remaining)
            price_data = fetch_price_data(token, end_time_ms - 60000, limit)
            print(f"fill gap price_data {limit}")
            
            save_to_mongodb(collection, price_data, token)
            batch_count = len(price_data)
            fetched += batch_count
            print(f"fetched {fetched}")
            
            time.sleep(SLEEP_TIME)
        
        print(f"Filled {fetched} minutes for {token}.")

def live_update():
    """Continuously fetch new data at minute intervals."""
    while True:
        current = datetime.now(timezone.utc)
        next_min = (current + timedelta(minutes=1)).replace(second=0, microsecond=0)
        sleep_sec = (next_min - current).total_seconds()
        time.sleep(max(0, sleep_sec))
        
        time.sleep(2)  # Wait for data availability
        
        for token in TOKENS:
            collection = db[f"{token}_timeseries"]
            prev_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            end_time = int(prev_min.timestamp() * 1000)
            
            price_data = fetch_price_data(token, end_time - 60000, 1)
            retries = 0
            while not price_data and retries < 3:
                print(f"Retrying {token} at {prev_min}...")
                time.sleep(2)
                price_data = fetch_price_data(token, end_time - 60000, 1)
                retries += 1
            
            if price_data:
                save_to_mongodb(collection, price_data, token)
                print(f"Updated {token} at {prev_min}")
            else:
                print(f"Failed to update {token} at {prev_min}")
            
            delete_oldest_records(collection)

if __name__ == "__main__":
    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        create_time_series_collection()
        fetch_initial_data()
        
        # Wait until next full minute
        now = datetime.now(timezone.utc)
        wait_sec = 60 - now.second
        print(f"Waiting {wait_sec} seconds to start gap filling...")
        time.sleep(wait_sec)
        
        fill_gaps()
        live_update()
    except Exception as e:
        print(f"Error: {e}")