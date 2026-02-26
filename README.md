# 🔗 DeFi Blockchain ETL Platform

A production-grade data engineering pipeline for real-time Ethereum data ingestion, transformation, quantitative risk modeling, and live visualization.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│              Blockchain Data Sources                     │
│     (Ethereum RPC, The Graph, Alchemy)                  │
└────────────────────────┬────────────────────────────────┘
                         │ gRPC / WebSocket
┌────────────────────────▼────────────────────────────────┐
│              Ingestion Layer (Kafka Producers)           │
│  • Block Producer  • TX Producer  • Event Log Producer  │
└────────────────────────┬────────────────────────────────┘
                         │ Kafka Topics
┌────────────────────────▼────────────────────────────────┐
│           ETL Transform Layer (Pandas + SQLGlot)        │
│   • ERC-20/ERC-721 Decoder  • DeFi Primitive Parser    │
│   • Cross-chain Normalizer  • Risk Feature Engineer     │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│           Quantitative Models & Risk Engine              │
│  • Portfolio VaR/CVaR  • Impermanent Loss Model         │
│  • MEV Exposure Scoring  • Protocol Risk Heuristics     │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│           Flask API + Interactive Showcase               │
│  • 4 live chart endpoints  • Self-contained HTML        │
└─────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Streaming | Kafka, gRPC, WebSocket, web3.py |
| ETL | Python, Pandas, SQLGlot |
| Blockchain | EVM (Ethereum), ERC-20, ERC-721, ERC-1155 |
| Risk Models | VaR, CVaR, Impermanent Loss, MEV Scoring |
| API | Flask, Flask-CORS |
| Data Warehouse | PostgreSQL, BigQuery, Snowflake (SQLGlot dialect translation) |

## Project Structure

```
blockchain_etl/
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── kafka_producer.py       # Kafka producer: blocks, txs, token transfer logs
│   ├── transform/
│   │   ├── __init__.py
│   │   └── etl_pipeline.py         # ETL: Pandas transforms + SQLGlot dialect builder
│   └── models/
│       ├── __init__.py
│       └── risk_models.py          # VaR, CVaR, Impermanent Loss, MEV, Protocol Risk
├── api/
│   ├── __init__.py
│   ├── server.py                   # Flask API server (4 endpoints)
│   └── data_service.py             # Calls real risk models & ETL pipeline
├── config/
│   └── config.example.yaml         # RPC, Kafka, DB, dialect config template
├── tests/
│   ├── __init__.py
│   └── test_etl_pipeline.py        # 38 pytest unit tests
├── scripts/
│   ├── run_pipeline.py             # Main entry point (live streaming)
│   └── backfill.py                 # Historical block backfill
├── blockchain_etl_showcase.html    # Interactive project showcase (self-contained)
└── requirements.txt
```

## Project Showcase

`blockchain_etl_showcase.html` is a self-contained interactive showcase — open directly in any browser or deploy to GitHub Pages / Render for a shareable URL.

Includes:
- Live pipeline architecture diagram
- Interactive file tree
- Full tech stack breakdown
- Tabbed code snippets (SQLGlot, VaR, Impermanent Loss, Kafka)
- Four live visualizations powered by the Flask API:
  - **Rolling VaR & CVaR** — 30-day historical simulation on a $1M ETH position
  - **Impermanent Loss Curve** — LP vs hold value across price ratios (Uniswap V2 AMM)
  - **MEV Risk Heatmap** — block-by-block MEV exposure scores
  - **Token Transfer Volume** — hourly ERC-20 and ERC-721 decoded from Kafka stream

- Four interactive data visualizations:
  - **Rolling VaR & CVaR** — 30-day historical simulation on a $1M ETH position
  - **Impermanent Loss Curve** — LP vs hold value across price ratios (Uniswap V2 AMM)
  - **MEV Risk Heatmap** — block-by-block MEV exposure scores
  - **Token Transfer Volume** — hourly ERC-20 and ERC-721 decoded from Kafka stream

## Quickstart

```bash
# 1. Create and activate virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
source venv/bin/activate       # Mac/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run tests
pytest tests/ -v               # 38/38 expected

# 4. Copy and configure
cp config/config.example.yaml config/config.yaml
# Edit config.yaml: set RPC_URL and KAFKA_BOOTSTRAP_SERVERS

# 5. Start the Flask API (powers the frontend visualizations)
python api/server.py
# Running at http://localhost:5000

# 6. Open the showcase
# Double-click blockchain_etl_showcase.html — charts load live from API

# 7. Run the live pipeline (requires RPC URL + Kafka)
python scripts/run_pipeline.py --network mainnet

# 8. Backfill historical blocks
python scripts/backfill.py --start-block 18000000 --end-block 18001000
```

## Deploying for a Live Resume Link

Push to GitHub, then deploy to [Render](https://render.com):

- **Build command:** `pip install -r requirements.txt`
- **Start command:** `python api/server.py`
- Update `const API` in `blockchain_etl_showcase.html` to your Render URL