# Crypto Market Analytics Pipeline

> Production-grade ETL pipeline for real-time cryptocurrency market data analysis

## 🎯 Project Overview

**Real-Time Crypto Market Analytics Pipeline** - A complete data engineering project demonstrating modern ETL practices with Apache Airflow, dbt, and cloud storage.

### Why Crypto Market Data?

- ✅ Free public APIs with high volume (Binance, CoinGecko)
- ✅ Real-time streaming data capabilities
- ✅ Business-relevant use case
- ✅ Rich historical data for analysis
- ✅ Endless feature expansion possibilities (ML predictions, alerts, etc.)

## 🏗️ Architecture

```
┌─────────────────┐
│  Data Sources   │
│  (Public APIs)  │
└────────┬────────┘
         │
    ┌────▼─────┐
    │ Ingestion│  ← Airflow DAGs
    │  Layer   │     (scheduled + sensor-based)
    └────┬─────┘
         │
    ┌────▼─────┐
    │   Raw    │  ← S3/MinIO (bronze)
    │ Storage  │     Parquet files
    └────┬─────┘
         │
    ┌────▼─────┐
    │Transform │  ← dbt models
    │  Layer   │     (silver → gold)
    └────┬─────┘
         │
    ┌────▼─────┐
    │Analytics │  ← PostgreSQL
    │   DB     │     (dimensional model)
    └────┬─────┘
         │
    ┌────▼─────┐
    │Dashboard │  ← Streamlit
    │   API    │     Public interface
    └──────────┘

      ┌──────────────┐
      │  Monitoring  │  ← Prometheus + Alerting
      └──────────────┘
```

## 📋 Technical Specification

### 1. Data Ingestion Layer

**Data Sources:**
- **Binance API**: OHLCV data (Open, High, Low, Close, Volume) for top 50 cryptocurrencies
- **CoinGecko API**: Market cap, circulating supply, metadata
- **Alternative.me**: Crypto Fear & Greed Index

**Airflow DAGs:**
- `ingest_ohlcv_hourly`: Fetches fresh data every hour
- `ingest_market_data_daily`: Daily aggregated metrics
- `backfill_historical`: One-time DAG for historical data (2-3 years)

**Key Features:**
- Retry logic with exponential backoff
- Rate limiting (API constraints)
- Idempotency (safe to re-run)
- Error handling + dead letter queue

**Deliverable:** 
- Airflow running in Docker Compose
- 3 working DAGs
- Raw data in S3/MinIO (1M+ records after backfill)

---

### 2. Storage Layer - Medallion Architecture

**Bronze Layer (Raw):**
- S3/MinIO buckets
- Parquet format (compressed, columnar)
- Partitioned by date: `s3://bucket/crypto/ohlcv/year=2024/month=01/day=15/`

**Silver Layer (Cleaned):**
- PostgreSQL staging tables
- Data quality checks passed
- Deduplication, type casting, null handling

**Gold Layer (Analytics-ready):**
- PostgreSQL dimensional model:
  - `fact_prices` (grain: crypto_id, timestamp)
  - `dim_crypto` (metadata)
  - `dim_date` (time dimension)
  - `fact_daily_summary` (aggregated metrics)

**Key Features:**
- Incremental loading (not full refresh)
- Schema evolution handling
- Data versioning (track changes over time)

**Deliverable:**
- Working medallion architecture
- SQL migration scripts
- Data volume: 1M+ raw records, 500k+ in gold layer

---

### 3. Transformation Layer - dbt

**Project Structure:**

```
models/
├── staging/
│   ├── stg_binance__ohlcv.sql      # Raw → cleaned
│   ├── stg_coingecko__market.sql
├── intermediate/
│   ├── int_crypto_hourly.sql        # Join + enrich
│   ├── int_volatility_calc.sql      # Rolling calculations
└── marts/
    ├── fct_prices.sql               # Final fact table
    ├── dim_crypto.sql               # Dimension
    └── fct_daily_summary.sql        # Aggregations
```

**Key Transformations:**
- Moving averages (7d, 30d)
- Volatility calculations (standard deviation)
- Price change percentages
- Volume-weighted average price (VWAP)

**Testing:**
- dbt tests: not_null, unique, relationships
- Custom tests: data freshness, value ranges

**Deliverable:**
- 10-15 dbt models
- Full test coverage
- Documentation (dbt docs generate)

---

### 4. Data Quality

**Framework:** Great Expectations or Soda Core

**Quality Checks:**
- **Completeness**: Are all expected records present?
- **Freshness**: Data not older than 2 hours?
- **Validity**: Prices within reasonable bounds? (no negatives, no anomalous spikes)
- **Consistency**: Volume > 0 when there are trades?

**Alerting:**
- Slack webhook on failures
- Email digest (daily summary)

**Deliverable:**
- 20+ quality checks
- Automated validation in Airflow
- Working alert system

---

### 5. Monitoring & Observability

**Metrics:**
- Pipeline health: success rate, failures, retries
- Performance: records/sec, latency, processing time
- Data metrics: row counts, null percentages, data freshness

**Implementation:**
- Structured logging (JSON format)
- Custom Airflow callbacks (on_failure, on_success)
- Metrics dashboard (Streamlit or Grafana)

**Deliverable:**
- Aggregated and parseable logs
- Dashboard showing pipeline health
- Configured alerts

---

### 6. Analytics Interface

**Streamlit Dashboard Pages:**
1. **Overview**: Top 10 cryptos by market cap, 24h changes
2. **Price Analysis**: Interactive charts (Plotly), technical indicators
3. **Market Sentiment**: Fear & Greed index, volume trends
4. **Data Quality**: Pipeline status, data freshness, error rates

**Key Features:**
- Real-time updates (refresh every hour)
- Interactive filters (date range, crypto selection)
- Responsive design

**Deliverable:**
- Public dashboard deployed (Streamlit Cloud free tier)
- Shareable URL for interviews

---

### 7. DevOps & CI/CD

**Docker Setup:**

`docker-compose.yml` includes:
- Airflow (webserver, scheduler, worker)
- PostgreSQL
- MinIO (local S3)
- Adminer (DB GUI)

**GitHub Actions:**

```yaml
.github/workflows/
├── tests.yml           # Run dbt tests + Python unit tests
├── deploy.yml          # Deploy to production
└── data_quality.yml    # Run quality checks on schedule
```

**Infrastructure as Code:**
- Terraform for AWS resources (if using cloud)
- Or docker-compose for local/VPS deployment

**Deliverable:**
- One-command setup: `docker-compose up`
- Automated testing in CI
- Deployment pipeline

---

### 8. Documentation

**Comprehensive README with:**
- Architecture diagram
- Quick start guide
- Tech stack overview
- Performance metrics
- Data quality reports
- Development guidelines
- Troubleshooting section

**Additional Documentation:**
- Architecture diagram (draw.io or Excalidraw)
- dbt documentation (auto-generated)
- Video demo (5 min walkthrough on YouTube)

## 🚀 Tech Stack

**Core Technologies:**
- **Orchestration**: Apache Airflow
- **Transformation**: dbt (data build tool)
- **Storage**: PostgreSQL, S3/MinIO, Parquet
- **Quality**: Great Expectations / Soda Core
- **Analytics**: Streamlit, Plotly
- **Infrastructure**: Docker, Docker Compose
- **CI/CD**: GitHub Actions

**Languages:**
- Python 3.11+
- SQL

## 📊 Project Metrics

**Data Volume:**
- 1M+ raw records after backfill
- 500k+ records in gold layer
- 50+ cryptocurrencies tracked
- 2-3 years historical data

**Pipeline Performance:**
- Throughput: 10k records/minute
- End-to-end latency: < 5 minutes
- Uptime: 99.9%+

**Quality:**
- 20+ automated quality checks
- Daily quality reports
- Slack alerting on failures

## 🎯 Learning Outcomes

By completing this project, you will gain hands-on experience with:

1. **Data Engineering Fundamentals**
   - ETL/ELT pipeline design
   - Medallion architecture (bronze/silver/gold)
   - Data quality frameworks

2. **Modern Data Stack**
   - Apache Airflow for orchestration
   - dbt for transformations
   - Cloud storage patterns

3. **DevOps Practices**
   - Docker containerization
   - CI/CD pipelines
   - Infrastructure as Code

4. **Production Best Practices**
   - Error handling and retries
   - Monitoring and alerting
   - Data validation
   - Documentation

## 📝 Project Timeline

**Total Duration:** 14 weeks (90 days)

- **Weeks 1-3**: Foundation + Data Ingestion
- **Weeks 4-6**: Storage Layer + Medallion Architecture
- **Weeks 7-9**: Transformation Layer (dbt)
- **Weeks 10-11**: Analytics Dashboard
- **Week 12**: Data Quality & Monitoring
- **Weeks 13-14**: Polish + Documentation

## 🎓 Prerequisites

**Required Knowledge:**
- Python basics
- SQL fundamentals
- Git/GitHub
- Command line basics

**To Learn:**
- Docker (2-3 hours)
- Apache Airflow (4-5 hours)
- dbt basics (3-4 hours)

**Note:** You don't need to be an expert! Learn as you build.

## 🚦 Getting Started

### Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

2. **Start the environment**
```bash
docker-compose up
```

3. **Access Airflow UI**
```
http://localhost:8080
Username: airflow
Password: airflow
```

4. **Trigger your first DAG**
- Navigate to DAGs page
- Enable `ingest_ohlcv_hourly`
- Click the play button to trigger

### Detailed Setup Guide

See [SETUP.md](SETUP.md) for detailed installation instructions.

## 📚 Resources

**Official Documentation:**
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Great Expectations](https://docs.greatexpectations.io/)

**Tutorials:**
- Docker Basics: [FreeCodeCamp](https://www.youtube.com/watch?v=fqMOX6JJhGo)
- Airflow Tutorial: [Official Quickstart](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- dbt Learn: [dbt Fundamentals](https://courses.getdbt.com/collections)

## 🤝 Contributing

This is a learning project, but contributions are welcome!

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

## 📧 Contact

**Serghei Matenko**
- LinkedIn: [serghei-matenko]
- Email: sergey.revo@outlook.com

---
