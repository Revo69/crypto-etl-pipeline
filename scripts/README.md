# Scripts

Helper scripts for development and debugging.

## check_api_keys.py

Quick verification that Binance API keys are configured correctly.

**Usage:**

```bash
python scripts/check_api_keys.py
```

## verify_binance_api.py

Comprehensive test suite for Binance API endpoints.

**Usage:**

```bash
python scripts/verify_binance_api.py
```

**Tests:**

- Server connectivity
- Single ticker (BTC)
- Multiple tickers (top 10 crypto)
- OHLCV historical data
- Exchange information
- Data saving to JSON
