"""CryptoFeed Data API — FastAPI entry point.

Run:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

Docs:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers import billing, books, funding, health, metrics, symbols, trades

app = FastAPI(
    title="CryptoFeed API",
    description="""
## Institutional-grade crypto market microstructure data

CryptoFeed collects real-time order book snapshots, trades, and funding rates from
**Binance Futures**, **Bybit**, and **OKX** — normalized to a common schema and
stored as compressed Parquet files.

### Authentication
All endpoints require an `X-API-Key` header. Get a key at [cryptofeed.io/signup](https://cryptofeed.io/signup).

### Plans
| Plan | History | Rate limit |
|------|---------|------------|
| Free | 7 days | 10 req/min |
| Pro | 90 days | 120 req/min |
| Enterprise | 2 years | 1000 req/min |

### Data types
- **Trades** — every aggTrade (Binance), trade (Bybit/OKX) with nanosecond timestamps
- **Books** — L2 order book snapshots at 100ms intervals, up to 400 levels
- **Funding** — perpetual funding rates every 1s (mark price updates)
- **Metrics** — on-the-fly OBI, VPIN, trade imbalance
""",
    version="1.0.0",
    contact={"name": "CryptoFeed Support", "email": "support@cryptofeed.io"},
    license_info={"name": "Commercial"},
)

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = str(uuid.uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": type(exc).__name__},
    )


# Allow POST for billing endpoints
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Routers
app.include_router(billing.router)
app.include_router(health.router)
app.include_router(symbols.router, prefix="/v1")
app.include_router(trades.router, prefix="/v1")
app.include_router(books.router, prefix="/v1")
app.include_router(funding.router, prefix="/v1")
app.include_router(metrics.router, prefix="/v1")


@app.get("/", include_in_schema=False)
def root():
    return {
        "name": "CryptoFeed API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
