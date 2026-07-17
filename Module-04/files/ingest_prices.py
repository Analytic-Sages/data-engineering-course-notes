import os
import sys
import json
import argparse
import urllib.request
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# Load database credentials from .env file
load_dotenv()

def fetch_and_store_prices(conn, schema, tokens_to_fetch, limit_end_ts=None):
    """Fetches hourly historical prices from DefiLlama API and saves them to the DB."""
    cursor = conn.cursor()
    for token_address, start_ts, end_ts in tokens_to_fetch:
        coin_id = f"ethereum:{token_address.lower()}"
        
        # Calculate the number of hours (periods) between start and end timestamps
        seconds_diff = max(0, end_ts - start_ts)
        span_hours = max(1, int(seconds_diff / 3600) + 1)
        
        print(f"Fetching prices for {coin_id} (from {start_ts} to {end_ts}, span: {span_hours} hours)...")
        
        # Request historical prices from DefiLlama with span and period parameters
        url = f"https://coins.llama.fi/chart/{coin_id}?start={start_ts}&span={span_hours}&period=1h"
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                res_data = json.loads(response.read().decode())
                
            coin_data = res_data.get('coins', {}).get(coin_id, {})
            prices = coin_data.get('prices', [])
            print(f"Retrieved {len(prices)} price snapshots.")
            
            # Write to DB
            insert_count = 0
            for price_point in prices:
                ts = price_point.get('timestamp')
                price_usd = price_point.get('price')
                
                if ts is None or price_usd is None:
                    continue
                
                # Filter out timestamps beyond limit_end_ts if running in bounded mode
                if limit_end_ts and ts > limit_end_ts:
                    continue
                    
                # Convert to datetime and round to nearest hour
                dt = datetime.fromtimestamp(ts)
                dt_hour = dt.replace(minute=0, second=0, microsecond=0)
                
                cursor.execute(f"""
                    INSERT INTO {schema}.token_prices (token_address, price_date, price_usd)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token_address, price_date) 
                    DO UPDATE SET price_usd = EXCLUDED.price_usd;
                """, (token_address.lower(), dt_hour, price_usd))
                insert_count += 1
            
            conn.commit()
            print(f"Saved {insert_count} prices to '{schema}.token_prices'.")
            
        except Exception as e:
            print(f"Error fetching/saving prices for {coin_id}: {e}")
            
    cursor.close()

def main():
    # Setup simple CLI arguments
    parser = argparse.ArgumentParser(description="Fetch and store historical token prices from DefiLlama")
    parser.add_argument('--start', type=int, help="Start Unix timestamp (seconds)")
    parser.add_argument('--end', type=int, help="End Unix timestamp (seconds)")
    parser.add_argument('--backfill', action='store_true', help="Scan DB Transfer table to auto-determine start/end times")
    args = parser.parse_args()
    
    # Read database connection parameters
    pg_host = os.environ.get('ENVIO_PG_HOST', 'localhost')
    pg_port = os.environ.get('ENVIO_PG_PORT', '5432')
    pg_user = os.environ.get('ENVIO_PG_USER', 'postgres')
    pg_password = os.environ.get('ENVIO_PG_PASSWORD', 'postgrespassword')
    pg_database = os.environ.get('ENVIO_PG_DATABASE', 'postgres')
    schema = os.environ.get('ENVIO_PG_PUBLIC_SCHEMA', 'analytics')
    
    print(f"Connecting to database '{pg_database}' on {pg_host}:{pg_port}...")
    
    try:
        # Connect to DB (fallback from internal docker host 'db' to 'localhost' if running on host)
        try:
            conn = psycopg2.connect(
                host=pg_host, port=pg_port, user=pg_user, password=pg_password, database=pg_database
            )
        except Exception as e:
            if pg_host == 'db':
                print("Could not connect to host 'db'. Retrying with 'localhost'...")
                conn = psycopg2.connect(
                    host='localhost', port=pg_port, user=pg_user, password=pg_password, database=pg_database
                )
            else:
                raise e
                
        cursor = conn.cursor()
        
        # 1. Create table structure if not exists and ensure unique index exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.token_prices (
                token_address TEXT,
                price_date TIMESTAMP,
                price_usd NUMERIC,
                PRIMARY KEY (token_address, price_date)
            );
        """)
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS token_prices_uidx 
            ON {schema}.token_prices (token_address, price_date);
        """)
        conn.commit()
        
        # 2. Determine target tokens and time bounds
        tokens_to_fetch = []
        
        if args.start and args.end:
            # Mode A: Airflow/Stateless mode (use parameters directly)
            print(f"Running in stateless mode. Time range: {args.start} to {args.end}")
            cursor.execute(f'SELECT DISTINCT "tokenAddress" FROM {schema}."Transfer";')
            tokens_to_fetch = [(row[0], args.start, args.end) for row in cursor.fetchall() if row[0]]
        else:
            # Mode B: Backfill mode (scan DB for bounds)
            print("No explicit time parameters given or --backfill active. Scanning DB for bounds...")
            cursor.execute(f"""
                SELECT "tokenAddress", MIN("blockTimestamp"), MAX("blockTimestamp")
                FROM {schema}."Transfer"
                GROUP BY "tokenAddress";
            """)
            tokens_to_fetch = [row for row in cursor.fetchall() if row[0]]
        
            
        cursor.close()
        
        if not tokens_to_fetch:
            print("No tokens or transfer records found to process.")
            conn.close()
            return
            
        # 3. Fetch and insert price data using the helper function
        fetch_and_store_prices(conn, schema, tokens_to_fetch, limit_end_ts=args.end)
        
        conn.close()
        print("Ingestion script completed successfully.")
        
    except Exception as e:
        print(f"Database error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
