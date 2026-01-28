"""
Cryptocurrency Data Ingestion to PostgreSQL
Fetches data from Binance and saves to PostgreSQL database
Compatible with Airflow 3.x
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

# Cryptocurrencies to track
CRYPTO_SYMBOLS = [
    'BTCUSDT',  # Bitcoin
    'ETHUSDT',  # Ethereum
    'BNBUSDT',  # Binance Coin
    'SOLUSDT',  # Solana
    'XRPUSDT',  # Ripple
]

# Default arguments
default_args = {
    'owner': 'crypto-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_and_save_to_postgres(**context):
    """
    Fetch crypto data and save directly to PostgreSQL
    """
    print("=" * 70)
    print("🚀 Fetching crypto data and saving to PostgreSQL...")
    print("=" * 70)
    
    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    url = "https://api.binance.com/api/v3/ticker/24hr"
    
    saved_count = 0
    failed_count = 0
    
    for symbol in CRYPTO_SYMBOLS:
        try:
            print(f"\n📊 Processing {symbol}...")
            
            # Fetch data from Binance
            params = {"symbol": symbol}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract fields
            crypto_name = symbol.replace('USDT', '')
            price = float(data['lastPrice'])
            price_change = float(data['priceChange'])
            price_change_percent = float(data['priceChangePercent'])
            high_24h = float(data['highPrice'])
            low_24h = float(data['lowPrice'])
            volume_24h = float(data['volume'])
            quote_volume_24h = float(data['quoteVolume'])
            fetched_at = datetime.now()
            
            # Insert into PostgreSQL
            insert_query = """
                INSERT INTO crypto_prices (
                    symbol, price, price_change_24h, price_change_percent_24h,
                    high_24h, low_24h, volume_24h, quote_volume_24h, fetched_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                symbol,
                price,
                price_change,
                price_change_percent,
                high_24h,
                low_24h,
                volume_24h,
                quote_volume_24h,
                fetched_at
            ))
            
            saved_count += 1
            
            # Log
            change_emoji = "📈" if price_change_percent > 0 else "📉"
            print(f"   ✅ {crypto_name}: ${price:,.2f} {change_emoji} {price_change_percent:+.2f}% → DB")
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ API Error for {symbol}: {str(e)}")
            failed_count += 1
            continue
            
        except Exception as e:
            print(f"   ❌ Database Error for {symbol}: {str(e)}")
            failed_count += 1
            continue
    
    # Commit all inserts
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n{'=' * 70}")
    print(f"✅ Saved: {saved_count}/{len(CRYPTO_SYMBOLS)} cryptocurrencies")
    if failed_count > 0:
        print(f"❌ Failed: {failed_count}")
    print(f"{'=' * 70}")
    
    return {'saved': saved_count, 'failed': failed_count}


def check_database_stats(**context):
    """
    Show database statistics
    """
    print("\n" + "=" * 70)
    print("📊 DATABASE STATISTICS")
    print("=" * 70)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Total records
    total_query = "SELECT COUNT(*) FROM crypto_prices"
    total_records = postgres_hook.get_first(total_query)[0]
    
    # Records per crypto
    crypto_query = """
        SELECT symbol, COUNT(*) as count 
        FROM crypto_prices 
        GROUP BY symbol 
        ORDER BY count DESC
    """
    crypto_stats = postgres_hook.get_records(crypto_query)
    
    # Latest prices
    latest_query = """
        SELECT DISTINCT ON (symbol) 
            symbol, price, price_change_percent_24h, fetched_at
        FROM crypto_prices 
        ORDER BY symbol, fetched_at DESC
    """
    latest_prices = postgres_hook.get_records(latest_query)
    
    # Print stats
    print(f"\n📈 Total Records: {total_records:,}")
    
    print(f"\n📊 Records by Cryptocurrency:")
    for symbol, count in crypto_stats:
        crypto_name = symbol.replace('USDT', '')
        print(f"   {crypto_name:<10} {count:>6,} records")
    
    print(f"\n💰 Latest Prices:")
    print(f"{'Crypto':<10} {'Price':<15} {'24h Change':<15} {'Updated'}")
    print("-" * 70)
    for symbol, price, change, fetched in latest_prices:
        crypto_name = symbol.replace('USDT', '')
        change_emoji = "📈" if change > 0 else "📉"
        fetched_str = fetched.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{crypto_name:<10} ${float(price):<14,.2f} {change_emoji} {float(change):<13.2f}% {fetched_str}")
    
    print("=" * 70 + "\n")
    
    return total_records


# Define the DAG
with DAG(
    dag_id='ingest_crypto_to_postgres',
    default_args=default_args,
    description='Fetch cryptocurrency data and save to PostgreSQL',
    #schedule='@hourly',
    schedule='*/5 * * * *',
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['crypto', 'binance', 'postgres', 'production'],
) as dag:
    
    # Task 1: Fetch and save to PostgreSQL
    fetch_and_save = PythonOperator(
        task_id='fetch_and_save_to_postgres',
        python_callable=fetch_and_save_to_postgres,
    )
    
    # Task 2: Show database statistics
    show_stats = PythonOperator(
        task_id='show_database_stats',
        python_callable=check_database_stats,
    )
    
    # Define dependencies
    fetch_and_save >> show_stats