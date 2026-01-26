"""
Multi-Cryptocurrency Data Ingestion DAG
Fetches data for top 5 cryptocurrencies from Binance API
Compatible with Airflow 3.x
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

# Top cryptocurrencies to track
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


def fetch_multiple_tickers(**context):
    """
    Fetch ticker data for multiple cryptocurrencies
    """
    print("=" * 70)
    print("🚀 Starting multi-crypto ticker fetch...")
    print("=" * 70)
    
    url = "https://api.binance.com/api/v3/ticker/24hr"
    
    all_data = []
    
    for symbol in CRYPTO_SYMBOLS:
        try:
            print(f"\n📊 Fetching {symbol}...")
            
            params = {"symbol": symbol}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            data['fetched_at'] = datetime.now().isoformat()
            data['dag_run_id'] = context['run_id']
            data['logical_date'] = context['logical_date'].isoformat()
            
            all_data.append(data)
            
            # Log key metrics
            crypto_name = symbol.replace('USDT', '')
            price = float(data['lastPrice'])
            change = float(data['priceChangePercent'])
            volume = float(data['volume'])
            
            change_emoji = "📈" if change > 0 else "📉"
            print(f"   ✅ {crypto_name}: ${price:,.2f} {change_emoji} {change:+.2f}% | Vol: {volume:,.0f}")
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error fetching {symbol}: {str(e)}")
            # Continue with other symbols even if one fails
            continue
    
    print(f"\n✅ Successfully fetched {len(all_data)}/{len(CRYPTO_SYMBOLS)} cryptocurrencies")
    print("=" * 70)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='ticker_data', value=all_data)
    
    return all_data


def save_tickers_to_json(**context):
    """
    Save all ticker data to JSON files
    """
    print("=" * 70)
    print("💾 Saving ticker data...")
    print("=" * 70)
    
    # Get data from previous task
    ti = context['task_instance']
    all_data = ti.xcom_pull(task_ids='fetch_tickers', key='ticker_data')
    
    if not all_data:
        raise ValueError("No ticker data received")
    
    # Create base directory
    base_dir = '/opt/airflow/data/crypto_tickers'
    os.makedirs(base_dir, exist_ok=True)
    
    logical_date = context['logical_date']
    timestamp = logical_date.strftime('%Y%m%d_%H%M%S')
    
    saved_files = []
    
    # Save each cryptocurrency to separate file
    for data in all_data:
        symbol = data['symbol']
        crypto_name = symbol.replace('USDT', '').lower()
        
        # Create subdirectory for each crypto
        crypto_dir = os.path.join(base_dir, crypto_name)
        os.makedirs(crypto_dir, exist_ok=True)
        
        # Save file
        filename = f"{crypto_name}_ticker_{timestamp}.json"
        filepath = os.path.join(crypto_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        file_size = os.path.getsize(filepath)
        saved_files.append(filepath)
        
        print(f"   ✅ {crypto_name.upper()}: {filename} ({file_size} bytes)")
    
    print(f"\n✅ Saved {len(saved_files)} files")
    print("=" * 70)
    
    return saved_files


def fetch_multiple_ohlcv(**context):
    """
    Fetch OHLCV data for multiple cryptocurrencies
    """
    print("=" * 70)
    print("📊 Fetching OHLCV data for all cryptos...")
    print("=" * 70)
    
    url = "https://api.binance.com/api/v3/klines"
    
    all_ohlcv = {}
    
    for symbol in CRYPTO_SYMBOLS:
        try:
            print(f"\n📈 Fetching {symbol} candles...")
            
            params = {
                "symbol": symbol,
                "interval": "1h",
                "limit": 24  # Last 24 hours
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            raw_data = response.json()
            
            # Parse candles
            ohlcv_data = []
            for candle in raw_data:
                ohlcv_data.append({
                    'timestamp': candle[0],
                    'datetime': datetime.fromtimestamp(candle[0] / 1000).isoformat(),
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5]),
                })
            
            all_ohlcv[symbol] = ohlcv_data
            
            # Stats
            prices = [d['close'] for d in ohlcv_data]
            crypto_name = symbol.replace('USDT', '')
            print(f"   ✅ {crypto_name}: {len(ohlcv_data)} candles | High: ${max(prices):,.2f} | Low: ${min(prices):,.2f}")
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error fetching {symbol}: {str(e)}")
            continue
    
    print(f"\n✅ Fetched OHLCV for {len(all_ohlcv)}/{len(CRYPTO_SYMBOLS)} cryptocurrencies")
    print("=" * 70)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='ohlcv_data', value=all_ohlcv)
    
    return all_ohlcv


def save_ohlcv_to_json(**context):
    """
    Save OHLCV data to JSON files
    """
    print("=" * 70)
    print("💾 Saving OHLCV data...")
    print("=" * 70)
    
    # Get data from previous task
    ti = context['task_instance']
    all_ohlcv = ti.xcom_pull(task_ids='fetch_ohlcv', key='ohlcv_data')
    
    if not all_ohlcv:
        raise ValueError("No OHLCV data received")
    
    # Create base directory
    base_dir = '/opt/airflow/data/crypto_ohlcv'
    os.makedirs(base_dir, exist_ok=True)
    
    logical_date = context['logical_date']
    timestamp = logical_date.strftime('%Y%m%d_%H%M%S')
    
    saved_files = []
    
    # Save each cryptocurrency
    for symbol, ohlcv_data in all_ohlcv.items():
        crypto_name = symbol.replace('USDT', '').lower()
        
        # Create subdirectory
        crypto_dir = os.path.join(base_dir, crypto_name)
        os.makedirs(crypto_dir, exist_ok=True)
        
        # Prepare output
        output = {
            'metadata': {
                'fetched_at': datetime.now().isoformat(),
                'logical_date': logical_date.isoformat(),
                'dag_run_id': context['run_id'],
                'symbol': symbol,
                'interval': '1h',
                'candles_count': len(ohlcv_data)
            },
            'data': ohlcv_data
        }
        
        # Save file
        filename = f"{crypto_name}_ohlcv_{timestamp}.json"
        filepath = os.path.join(crypto_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2)
        
        file_size = os.path.getsize(filepath)
        saved_files.append(filepath)
        
        print(f"   ✅ {crypto_name.upper()}: {len(ohlcv_data)} candles ({file_size} bytes)")
    
    print(f"\n✅ Saved {len(saved_files)} files")
    print("=" * 70)
    
    return saved_files


def print_summary(**context):
    """
    Print summary of the DAG run
    """
    print("\n" + "=" * 70)
    print("📊 MULTI-CRYPTO DAG RUN SUMMARY")
    print("=" * 70)
    
    ti = context['task_instance']
    
    # Get ticker data
    ticker_data = ti.xcom_pull(task_ids='fetch_tickers', key='ticker_data')
    if ticker_data:
        print(f"\n💰 Ticker Data: {len(ticker_data)} cryptocurrencies")
        print(f"\n{'Crypto':<10} {'Price':<15} {'24h Change':<15} {'Volume'}")
        print("-" * 70)
        
        for data in ticker_data:
            symbol = data['symbol'].replace('USDT', '')
            price = float(data['lastPrice'])
            change = float(data['priceChangePercent'])
            volume = float(data['volume'])
            
            change_emoji = "📈" if change > 0 else "📉"
            print(f"{symbol:<10} ${price:<14,.2f} {change_emoji} {change:<13.2f}% {volume:,.0f}")
    
    # Get OHLCV data
    ohlcv_data = ti.xcom_pull(task_ids='fetch_ohlcv', key='ohlcv_data')
    if ohlcv_data:
        total_candles = sum(len(candles) for candles in ohlcv_data.values())
        print(f"\n📈 OHLCV Data:")
        print(f"   Cryptocurrencies: {len(ohlcv_data)}")
        print(f"   Total candles: {total_candles}")
        print(f"   Time range: 24 hours per crypto")
    
    print(f"\n✅ DAG completed successfully!")
    print(f"   Logical date: {context['logical_date']}")
    print(f"   Run ID: {context['run_id']}")
    print(f"   Total records: ~{len(ticker_data) * 25} (tickers + candles)")
    print("=" * 70 + "\n")


# Define the DAG
with DAG(
    dag_id='ingest_multi_crypto_hourly',
    default_args=default_args,
    description='Fetch data for top 5 cryptocurrencies from Binance every hour',
    schedule='@hourly',
    start_date=datetime(2026, 1, 18),
    catchup=False,
    tags=['crypto', 'binance', 'multi', 'ingestion'],
) as dag:
    
    # Task 1: Fetch all tickers
    fetch_tickers = PythonOperator(
        task_id='fetch_tickers',
        python_callable=fetch_multiple_tickers,
    )
    
    # Task 2: Save tickers
    save_tickers = PythonOperator(
        task_id='save_tickers',
        python_callable=save_tickers_to_json,
    )
    
    # Task 3: Fetch OHLCV
    fetch_ohlcv = PythonOperator(
        task_id='fetch_ohlcv',
        python_callable=fetch_multiple_ohlcv,
    )
    
    # Task 4: Save OHLCV
    save_ohlcv = PythonOperator(
        task_id='save_ohlcv',
        python_callable=save_ohlcv_to_json,
    )
    
    # Task 5: Summary
    summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
    )
    
    # Define dependencies
    fetch_tickers >> save_tickers >> summary
    fetch_ohlcv >> save_ohlcv >> summary