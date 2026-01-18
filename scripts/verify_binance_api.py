"""
Binance API Test Script
Tests all endpoints needed for the ETL pipeline
"""

import os
from dotenv import load_dotenv
import requests
from datetime import datetime
import json

load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

BASE_URL = "https://api.binance.com"

def test_server_time():
    """Test 1: Check if Binance API is accessible"""
    print("\n" + "="*60)
    print("TEST 1: Server Time (API Health Check)")
    print("="*60)
    
    url = f"{BASE_URL}/api/v3/time"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        server_time = datetime.fromtimestamp(data['serverTime'] / 1000)
        print(f"✅ Binance API is accessible")
        print(f"   Server Time: {server_time}")
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def test_single_ticker():
    """Test 2: Get BTC price"""
    print("\n" + "="*60)
    print("TEST 2: Single Ticker (BTC Price)")
    print("="*60)
    
    url = f"{BASE_URL}/api/v3/ticker/24hr"
    params = {"symbol": "BTCUSDT"}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Successfully fetched BTC data")
        print(f"   Symbol: {data['symbol']}")
        print(f"   Price: ${float(data['lastPrice']):,.2f}")
        print(f"   24h Change: {float(data['priceChangePercent']):.2f}%")
        print(f"   24h Volume: {float(data['volume']):,.2f} BTC")
        print(f"   24h High: ${float(data['highPrice']):,.2f}")
        print(f"   24h Low: ${float(data['lowPrice']):,.2f}")
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def test_multiple_tickers():
    """Test 3: Get top 10 cryptocurrencies"""
    print("\n" + "="*60)
    print("TEST 3: Multiple Tickers (Top 10 Crypto)")
    print("="*60)
    
    # Popular trading pairs
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
               "ADAUSDT", "DOGEUSDT", "MATICUSDT", "DOTUSDT", "LTCUSDT"]
    
    url = f"{BASE_URL}/api/v3/ticker/24hr"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        all_data = response.json()
        
        # Filter for our symbols
        crypto_data = [d for d in all_data if d['symbol'] in symbols]
        
        print(f"✅ Successfully fetched {len(crypto_data)} cryptocurrencies")
        print(f"\n{'Symbol':<12} {'Price':<15} {'24h Change':<12} {'Volume'}")
        print("-" * 60)
        
        for coin in crypto_data[:10]:
            symbol = coin['symbol'].replace('USDT', '')
            price = float(coin['lastPrice'])
            change = float(coin['priceChangePercent'])
            volume = float(coin['quoteVolume'])
            
            change_emoji = "📈" if change > 0 else "📉"
            print(f"{symbol:<12} ${price:<14,.2f} {change_emoji} {change:<10.2f}% ${volume:,.0f}")
        
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def test_ohlcv_data():
    """Test 4: Get historical OHLCV (klines) data"""
    print("\n" + "="*60)
    print("TEST 4: OHLCV Data (Historical Klines)")
    print("="*60)
    
    url = f"{BASE_URL}/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1h",  # 1 hour candles
        "limit": 24        # Last 24 hours
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Successfully fetched {len(data)} hourly candles")
        print(f"\n{'Time':<20} {'Open':<12} {'High':<12} {'Low':<12} {'Close':<12} {'Volume'}")
        print("-" * 85)
        
        # Show last 5 candles
        for candle in data[-5:]:
            timestamp = datetime.fromtimestamp(candle[0] / 1000)
            open_price = float(candle[1])
            high = float(candle[2])
            low = float(candle[3])
            close = float(candle[4])
            volume = float(candle[5])
            
            print(f"{timestamp.strftime('%Y-%m-%d %H:%M'):<20} "
                  f"${open_price:<11,.2f} ${high:<11,.2f} ${low:<11,.2f} "
                  f"${close:<11,.2f} {volume:,.2f}")
        
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def test_exchange_info():
    """Test 5: Get exchange information (available pairs)"""
    print("\n" + "="*60)
    print("TEST 5: Exchange Info (Available Trading Pairs)")
    print("="*60)
    
    url = f"{BASE_URL}/api/v3/exchangeInfo"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        symbols = data['symbols']
        
        # Filter USDT pairs
        usdt_pairs = [s for s in symbols if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']
        
        print(f"✅ Exchange Info retrieved")
        print(f"   Total Trading Pairs: {len(symbols)}")
        print(f"   USDT Pairs: {len(usdt_pairs)}")
        print(f"   Server Time: {datetime.fromtimestamp(data['serverTime'] / 1000)}")
        
        # Show some popular pairs
        print(f"\n   Sample USDT pairs:")
        for pair in usdt_pairs[:10]:
            print(f"   - {pair['symbol']}")
        
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def test_api_key_validity():
    """Test 6: Verify API Key is configured (optional)"""
    print("\n" + "="*60)
    print("TEST 6: API Key Configuration Check")
    print("="*60)
    
    if API_KEY and SECRET_KEY:
        print(f"✅ API Key configured")
        print(f"   API Key: {API_KEY[:8]}...{API_KEY[-8:]}")
        print(f"   Secret Key: {SECRET_KEY[:8]}...{'*' * 8}")
        print(f"\n   Note: API key is not required for public market data")
        print(f"   We're only using public endpoints for this project!")
        return True
    else:
        print(f"⚠️  API Key not found in .env file")
        print(f"   This is OK - we only need public endpoints!")
        return True


def save_sample_data():
    """Test 7: Save sample data to file (simulate ETL)"""
    print("\n" + "="*60)
    print("TEST 7: Save Sample Data (ETL Simulation)")
    print("="*60)
    
    # Fetch data
    url = f"{BASE_URL}/api/v3/ticker/24hr"
    params = {"symbol": "BTCUSDT"}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Add metadata
        data['fetched_at'] = datetime.now().isoformat()
        data['source'] = 'binance_api'
        
        # Save to file
        filename = f"sample_btc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Sample data saved to: {filename}")
        print(f"   File size: {os.path.getsize(filename)} bytes")
        print(f"   This simulates what our Airflow DAG will do!")
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print(" 🚀 BINANCE API TEST SUITE FOR ETL PIPELINE")
    print("="*60)
    print(f" Testing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    tests = [
        test_server_time,
        test_single_ticker,
        test_multiple_tickers,
        test_ohlcv_data,
        test_exchange_info,
        test_api_key_validity,
        save_sample_data
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            results.append(False)
    
    # Summary
    print("\n" + "="*60)
    print(" 📊 TEST SUMMARY")
    print("="*60)
    passed = sum(results)
    total = len(results)
    print(f" Tests Passed: {passed}/{total}")
    print(f" Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("\n 🎉 ALL TESTS PASSED! Ready to build the ETL pipeline!")
    else:
        print(f"\n ⚠️  {total - passed} test(s) failed. Check errors above.")
    
    print("="*60)


if __name__ == "__main__":
    main()