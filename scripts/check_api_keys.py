import os
from dotenv import load_dotenv
import requests
import time
import hmac
import hashlib

load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

# Test 1: Public endpoint (без ключа)
print("Test 1: Public API")
response = requests.get(
    "https://api.binance.com/api/v3/ticker/24hr",
    params={"symbol": "BTCUSDT"}
)
if response.status_code == 200:
    print("✅ Public API works")
    print(f"BTC: ${response.json()['lastPrice']}")
else:
    print(f"❌ Error: {response.status_code}")

# Test 2: API Key verification
print("\nTest 2: API Key validation")
headers = {'X-MBX-APIKEY': API_KEY}
response = requests.get(
    "https://api.binance.com/api/v3/account",
    headers=headers,
    params={
        'timestamp': int(time.time() * 1000),
        'recvWindow': 5000
    }
)

if response.status_code == 200:
    print("✅ API Key works!")
elif response.status_code == 401:
    print("❌ Invalid API Key")
elif response.status_code == -1022:
    print("⚠️ Need signature (but API key is valid)")
else:
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text[:200]}")