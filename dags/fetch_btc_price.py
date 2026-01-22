from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

def fetch_and_save_btc():
    # Fetch data
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {"symbol": "BTCUSDT"}
    response = requests.get(url, params=params)
    data = response.json()
    
    # Add timestamp
    data['fetched_at'] = datetime.now().isoformat()
    
    # Save to file
    filename = f"/tmp/btc_price_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved to: {filename}")
    print(f"BTC Price: ${data['lastPrice']}")

with DAG(
    dag_id='fetch_btc_price',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',  # Run every hour
    catchup=False
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_btc_data',
        python_callable=fetch_and_save_btc
    )