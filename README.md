# cryptofeed-pipeline

A production-grade crypto market microstructure data pipeline — a self-hosted alternative to Tardis.dev.

Collects real-time order book snapshots, trades, and funding rates from Binance Futures, Bybit, and OKX via WebSocket. Normalizes all data to a common schema and stores it as partitioned Parquet files. Includes a deterministic replay engine for backtesting.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                     COLLECTOR                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  Binance    │  │   Bybit     │  │    OKX      │  │
│  │  Futures    │  │  Linear     │  │  Futures    │  │
│  │  WebSocket  │  │  WebSocket  │  │  WebSocket  │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │
│         │                │                │          │
│         └────────────────┼────────────────┘          │
│                          ▼                           │
│              ┌───────────────────────┐               │
│              │   OrderBookEngine     │               │
│              │  (SortedDict L2 book) │               │
│              └───────────┬───────────┘               │
└──────────────────────────┼───────────────────────────┘
                           ▼
              ┌────────────────────────┐
              │      Redis Streams     │
              │  exchange:type:symbol  │
              └────────────┬───────────┘
                           ▼
┌──────────────────────────────────────────────────────┐
│                     PROCESSOR                        │
│  ┌─────────────────────────────────────────────────┐ │
│  │              Normalizer                         │ │
│  │  Raw exchange msg → NormalizedTrade/Book/Funding │ │
│  └───────────────────────┬─────────────────────────┘ │
│                          ▼                           │
│  ┌─────────────────────────────────────────────────┐ │
│  │              ParquetWriter                      │ │
│  │  data/{type}/{exchange}/{symbol}/YYYY-MM-DD/HH/ │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
                           ▼
              ┌────────────────────────┐
              │     Parquet Store      │
              │  zstd compressed,      │
              │  atomic writes,        │
              │  partitioned by hour   │
              └────────────┬───────────┘
                           ▼
              ┌────────────────────────┐
              │    ReplayEngine        │
              │  Deterministic replay  │
              │  for backtesting       │
              └────────────────────────┘
```

## Quick Start

### Local (no Docker)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env — at minimum set SYMBOLS and which exchanges to enable

# 3. Start Redis (requires Docker or local Redis install)
docker run -d -p 6379:6379 redis:7-alpine

# 4. Start collector (collects from exchanges → Redis)
python collector/main.py

# 5. Start processor (Redis → Parquet files)
python processor/main.py
```

### Docker Compose (recommended)

```bash
cp .env.example .env
docker-compose up -d
docker-compose logs -f
```

## Data Format

All data is stored as Parquet files partitioned by:
```
data/
  trades/
    binance/BTCUSDT/2025-03-17/13/data.parquet
    bybit/BTCUSDT/2025-03-17/13/data.parquet
    okx/BTCUSDT/2025-03-17/13/data.parquet
  books/
    binance/BTCUSDT/2025-03-17/13/data.parquet
  funding/
    binance/BTCUSDT/2025-03-17/13/data.parquet
```

### Trade schema
| Field | Type | Description |
|-------|------|-------------|
| exchange | str | binance / bybit / okx |
| symbol | str | BTCUSDT (normalized) |
| timestamp_ns | int64 | Nanoseconds since epoch |
| trade_id | str | Exchange trade ID |
| price | float64 | Trade price |
| qty | float64 | Trade quantity (base asset) |
| side | str | buy / sell |
| is_liquidation | bool | True if forced liquidation |

### Book snapshot schema
| Field | Type | Description |
|-------|------|-------------|
| exchange | str | |
| symbol | str | |
| timestamp_ns | int64 | |
| seq | int64 | Exchange sequence number |
| bids | list | [[price, qty], ...] top 20 levels |
| asks | list | [[price, qty], ...] top 20 levels |
| spread | float64 | Best ask - best bid |
| midprice | float64 | (best_bid + best_ask) / 2 |
| microprice | float64 | Volume-weighted midprice |
| obi_5 | float64 | Order book imbalance top 5 levels |

## Replay Engine

Use the ReplayEngine to replay historical data for backtesting:

```python
from datetime import datetime, timezone
from replay.engine import ReplayEngine

engine = ReplayEngine(data_dir="./data")

start = datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 3, 17, 1, 0, tzinfo=timezone.utc)

for event in engine.iter_merged("binance", "BTCUSDT", start, end):
    print(event)
```

## Microstructure Features

```python
import pandas as pd
from features.microstructure import compute_trade_imbalance, compute_obi

trade_df = pd.read_parquet("data/trades/binance/BTCUSDT/2025-03-17/13/data.parquet")
imbalance = compute_trade_imbalance(trade_df, window=100)
```

## Running Tests

```bash
pytest tests/ -v
```

## Selling / Licensing Data

The collected data can be packaged and sold similarly to Tardis.dev:
- All files are standard Parquet — no proprietary format lock-in
- Partition by exchange/symbol/date/hour makes it easy to ship subsets
- Add a simple HTTP server (FastAPI) on top to serve files on demand
