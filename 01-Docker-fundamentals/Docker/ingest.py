import requests
import pandas as pd
from sqlalchemy import create_engine
import os


host = os.getenv("DB_HOST", "localhost")

url = "https://blockchain.info/unconfirmed-transactions?format=json"

try:
    print("Fetching live Bitcoin transactions...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"Successfully fetched {len(data.get('txs', []))} live transactions!")
except Exception as e:
    print(f"\n[WARNING] Could not fetch live transactions: {e}")
    print("Falling back to mock transaction data...\n")
    
    # Standard mock data matching the API schema
    data = {
        "txs": [
            {"hash": "a10b2c3d4e5f6g7h8i9j0k1l2m3n4o5p", "time": 1783583400, "size": 250, "fee": 15000},
            {"hash": "b987654321fedcbaabcdef0123456789", "time": 1783583420, "size": 400, "fee": 22000},
            {"hash": "c0fefe00000000000000000000000000", "time": 1783583450, "size": 220, "fee": 12000}
        ]
    }

# Show the first transaction in the payload



# 1. Load data into DataFrame
df = pd.DataFrame(data['txs'])

# 2. Select key columns
columns_to_keep = ['hash', 'time', 'size', 'fee']
df = df[columns_to_keep]

# 3. Transform: Convert Unix timestamp (seconds) to a readable datetime format
df['timestamp'] = pd.to_datetime(df['time'], unit='s')

# 4. Add 0x prefix at the hash
df['hash'] = '0x'+ df['hash']

# Drop the original 'time' column since we have the parsed timestamp
df = df.drop(columns=['time'])

# Display the cleaned data
df.head()


# Create PostgreSQL connection engine
engine = create_engine(f'postgresql://root:root@{host}:5432/data-set')

# Load to SQL table 'bitcoin_transactions'
df.to_sql('bitcoin_transactions', con=engine, if_exists='replace', index=False)

print("Ingestion complete! Data successfully loaded into PostgreSQL.")