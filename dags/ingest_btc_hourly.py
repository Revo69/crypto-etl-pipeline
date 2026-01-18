"""
Binance BTC Data Ingestion DAG
Fetches Bitcoin price data every hour and saves to JSON
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'crypto-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_btc_ticker(**context):
    """
    Fetch BTC 24hr ticker data from Binance
    """
    print("=" * 60)
    print("Starting BTC ticker fetch...")
    print("=" * 60)
    
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {"symbol": "BTCUSDT"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Add metadata
        data['fetched_at'] = datetime.now().isoformat()
        data['dag_run_id'] = context['run_id']
        data['logical_date'] = context['logical_date'].isoformat()
        
        # Log key metrics
        print(f"✅ Successfully fetched BTC data")
        print(f"   Price: ${float(data['lastPrice']):,.2f}")
        print(f"   24h Change: {float(data['priceChangePercent']):.2f}%")
        print(f"   24h Volume: {float(data['volume']):,.2f} BTC")
        print(f"   24h High: ${float(data['highPrice']):,.2f}")
        print(f"   24h Low: ${float(data['lowPrice']):,.2f}")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='btc_data', value=data)
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {str(e)}")
        raise


def save_to_json(**context):
    """
    Save fetched data to JSON file
    """
    print("=" * 60)
    print("Saving data to JSON...")
    print("=" * 60)
    
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='fetch_btc_ticker', key='btc_data')
    
    if not data:
        raise ValueError("No data received from fetch task")
    
    # Create data directory if it doesn't exist
    data_dir = '/opt/airflow/data/btc_ticker'
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate filename with timestamp
    logical_date = context['logical_date']
    filename = f"btc_ticker_{logical_date.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(data_dir, filename)
    
    # Save to file
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    file_size = os.path.getsize(filepath)
    
    print(f"✅ Data saved successfully")
    print(f"   File: {filename}")
    print(f"   Path: {filepath}")
    print(f"   Size: {file_size} bytes")
    
    return filepath


def fetch_ohlcv_data(**context):
    """
    Fetch OHLCV (candlestick) data for last 24 hours
    """
    print("=" * 60)
    print("Fetching OHLCV data...")
    print("=" * 60)
    
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1h",
        "limit": 24  # Last 24 hours
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Parse klines data
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
                'close_time': candle[6],
                'quote_volume': float(candle[7]),
                'trades': candle[8]
            })
        
        print(f"✅ Fetched {len(ohlcv_data)} hourly candles")
        
        # Calculate basic stats
        prices = [d['close'] for d in ohlcv_data]
        print(f"   24h High: ${max(prices):,.2f}")
        print(f"   24h Low: ${min(prices):,.2f}")
        print(f"   Average: ${sum(prices)/len(prices):,.2f}")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='ohlcv_data', value=ohlcv_data)
        
        return ohlcv_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching OHLCV data: {str(e)}")
        raise


def save_ohlcv_to_json(**context):
    """
    Save OHLCV data to JSON file
    """
    print("=" * 60)
    print("Saving OHLCV data...")
    print("=" * 60)
    
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='fetch_ohlcv', key='ohlcv_data')
    
    if not data:
        raise ValueError("No OHLCV data received")
    
    # Create directory
    data_dir = '/opt/airflow/data/btc_ohlcv'
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate filename
    logical_date = context['logical_date']
    filename = f"btc_ohlcv_{logical_date.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(data_dir, filename)
    
    # Add metadata
    output = {
        'metadata': {
            'fetched_at': datetime.now().isoformat(),
            'logical_date': context['logical_date'].isoformat(),
            'dag_run_id': context['run_id'],
            'symbol': 'BTCUSDT',
            'interval': '1h',
            'candles_count': len(data)
        },
        'data': data
    }
    
    # Save to file
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    
    file_size = os.path.getsize(filepath)
    
    print(f"✅ OHLCV data saved")
    print(f"   File: {filename}")
    print(f"   Candles: {len(data)}")
    print(f"   Size: {file_size} bytes")
    
    return filepath


def print_summary(**context):
    """
    Print summary of the DAG run
    """
    print("\n" + "=" * 60)
    print("📊 DAG RUN SUMMARY")
    print("=" * 60)
    
    ti = context['task_instance']
    
    # Get ticker data
    ticker_data = ti.xcom_pull(task_ids='fetch_btc_ticker', key='btc_data')
    if ticker_data:
        print("\n💰 BTC Ticker:")
        print(f"   Price: ${float(ticker_data['lastPrice']):,.2f}")
        print(f"   24h Change: {float(ticker_data['priceChangePercent']):.2f}%")
        print(f"   Volume: {float(ticker_data['volume']):,.2f} BTC")
    
    # Get OHLCV data
    ohlcv_data = ti.xcom_pull(task_ids='fetch_ohlcv', key='ohlcv_data')
    if ohlcv_data:
        print(f"\n📈 OHLCV Data:")
        print(f"   Candles fetched: {len(ohlcv_data)}")
        print(f"   Time range: 24 hours")
    
    print(f"\n✅ DAG completed successfully!")
    print(f"   Logical date: {context['logical_date']}")
    print(f"   Run ID: {context['run_id']}")
    print("=" * 60 + "\n")


# Define the DAG
with DAG(
    dag_id='ingest_btc_hourly',
    default_args=default_args,
    description='Fetch Bitcoin data from Binance API every hour',
    schedule='@hourly',  # Run every hour
    start_date=datetime(2026, 1, 18),
    catchup=False,  # Don't backfill
    tags=['crypto', 'binance', 'btc', 'ingestion'],
) as dag:
    
    # Task 1: Fetch BTC ticker
    fetch_ticker = PythonOperator(
        task_id='fetch_btc_ticker',
        python_callable=fetch_btc_ticker,
    )
    
    # Task 2: Save ticker to JSON
    save_ticker = PythonOperator(
        task_id='save_ticker_json',
        python_callable=save_to_json,
    )
    
    # Task 3: Fetch OHLCV data
    fetch_ohlcv = PythonOperator(
        task_id='fetch_ohlcv',
        python_callable=fetch_ohlcv_data,
    )
    
    # Task 4: Save OHLCV to JSON
    save_ohlcv = PythonOperator(
        task_id='save_ohlcv_json',
        python_callable=save_ohlcv_to_json,
    )
    
    # Task 5: Print summary
    summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
    )
    
    # Define task dependencies
    # Fetch ticker -> Save ticker
    # Fetch OHLCV -> Save OHLCV
    # Both save tasks -> Summary
    fetch_ticker >> save_ticker >> summary
    fetch_ohlcv >> save_ohlcv >> summary