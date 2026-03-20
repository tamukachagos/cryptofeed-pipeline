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
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from api.routers import billing, books, funding, health, intelligence, internal, liquidations, mark_price, metrics, ohlcv, open_interest, symbols, trades

# ── Prometheus instrumentation ─────────────────────────────────────────────────
try:
    from prometheus_client import (
        Counter as _PCounter,
        Histogram as _PHistogram,
        generate_latest as _prom_generate,
        CONTENT_TYPE_LATEST as _PROM_CONTENT_TYPE,
        REGISTRY as _PROM_REGISTRY,
    )
    from fastapi.responses import Response as _Response
    _PROM_ENABLED = True
except ImportError:
    _PROM_ENABLED = False

if _PROM_ENABLED:
    _http_requests_total = _PCounter(
        "http_requests_total",
        "Total HTTP requests",
        ["method", "path", "status"],
    )
    _http_request_duration = _PHistogram(
        "http_request_duration_seconds",
        "HTTP request duration in seconds",
        ["method", "path"],
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    )
    _auth_failures_total = _PCounter(
        "auth_failures_total",
        "Total authentication failures",
        ["ip"],
    )

_MAX_BODY_BYTES = int(os.getenv("MAX_REQUEST_BODY_BYTES", 1_048_576))  # 1 MB default


class _BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests whose body exceeds the configured limit.

    Checks Content-Length header first (fast path), then enforces the limit
    by reading the actual stream — preventing bypass via omitted header.
    """

    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > _MAX_BODY_BYTES:
                    return JSONResponse(
                        status_code=413,
                        content={"detail": f"Request body too large (max {_MAX_BODY_BYTES // 1024} KB)"},
                    )
            except ValueError:
                pass

        # Also enforce on the actual body stream (prevents Content-Length omission bypass)
        if request.method in ("POST", "PUT", "PATCH"):
            body_bytes = b""
            async for chunk in request.stream():
                body_bytes += chunk
                if len(body_bytes) > _MAX_BODY_BYTES:
                    return JSONResponse(
                        status_code=413,
                        content={"detail": f"Request body too large (max {_MAX_BODY_BYTES // 1024} KB)"},
                    )
            # Re-inject the consumed body so downstream handlers can read it
            from starlette.datastructures import Headers
            async def _body_stream():
                yield body_bytes
            request._stream = _body_stream()
            request._body = body_bytes

        return await call_next(request)


# ── Environment + startup safety validation ────────────────────────────────────
_ENVIRONMENT           = os.getenv("ENVIRONMENT", "development").lower()
_STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
_STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
_BYPASS_AUTH           = os.getenv("BYPASS_AUTH", "false").lower() == "true"
_HMAC_REQUIRED         = os.getenv("HMAC_REQUIRED", "false").lower() == "true"

# ── PRODUCTION KILL-SWITCH: hard-fail on catastrophic misconfigurations ────────
if _ENVIRONMENT == "production":
    _fatal_errors: list[str] = []

    if _BYPASS_AUTH:
        _fatal_errors.append(
            "BYPASS_AUTH=true is set in production. This disables ALL authentication. "
            "This is a catastrophic misconfiguration."
        )
    if not os.getenv("SECRET_ENCRYPTION_KEY", "").strip():
        _fatal_errors.append(
            "SECRET_ENCRYPTION_KEY is not set. API secrets cannot be encrypted at rest."
        )
    if not _STRIPE_WEBHOOK_SECRET:
        _fatal_errors.append("STRIPE_WEBHOOK_SECRET not set — billing webhooks disabled")
    if not os.getenv("INTERNAL_API_SECRET", "").strip():
        _fatal_errors.append(
            "INTERNAL_API_SECRET not set — /internal/* endpoints are inaccessible (503)."
        )

    if _fatal_errors:
        for err in _fatal_errors:
            logger.error(f"[startup] FATAL CONFIG ERROR: {err}")
        # Hard-fail for BYPASS_AUTH only — others are warnings
        if _BYPASS_AUTH or not os.getenv("SECRET_ENCRYPTION_KEY", "").strip():
            import sys as _sys
            logger.error("[startup] System cannot start with unsafe production configuration. Exiting.")
            _sys.exit(1)
else:
    # Development/staging: warn but allow
    if not _STRIPE_WEBHOOK_SECRET:
        logger.warning("[startup] STRIPE_WEBHOOK_SECRET not set — billing webhooks disabled")
    if not _STRIPE_SECRET_KEY:
        logger.warning("[startup] STRIPE_SECRET_KEY not set — checkout disabled")
    if _BYPASS_AUTH:
        logger.warning("[startup] ⚠ BYPASS_AUTH=true — ALL endpoints open as enterprise. NOT for production!")


_DOCS_ENABLED = os.getenv("ENABLE_DOCS", "false").lower() == "true" or _BYPASS_AUTH

app = FastAPI(
    title="CryptoFeed API",
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
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
- **OHLCV** — 1-minute candlestick data from all exchanges
- **Open Interest** — total open interest updated every 60s
- **Mark Price** — mark price and index price at 1s intervals
- **Liquidations** — forced liquidation orders with price, size, and side
- **Metrics** — on-the-fly OBI, VPIN, trade imbalance
""",
    version="1.0.0",
    contact={"name": "CryptoFeed Support", "email": "support@cryptofeed.io"},
    license_info={"name": "Commercial"},
)

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    import time as _time
    request_id = str(uuid.uuid4())
    _t0 = _time.monotonic()
    response = await call_next(request)
    _elapsed = _time.monotonic() - _t0
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if _PROM_ENABLED:
        _path = request.url.path
        _http_requests_total.labels(request.method, _path, str(response.status_code)).inc()
        _http_request_duration.labels(request.method, _path).observe(_elapsed)
    return response


# ── Auth failure rate limiter (middleware-level, IP-based) ────────────────────
import collections as _collections
_auth_fail_windows: dict[str, _collections.deque] = {}
_AUTH_FAIL_LIMIT  = int(os.getenv("AUTH_FAIL_LIMIT", "20"))     # per IP per minute
_AUTH_FAIL_WINDOW = 60.0


@app.middleware("http")
async def auth_failure_throttle(request: Request, call_next):
    """Rate-limit IP addresses that produce repeated 401 responses."""
    import time as _time
    response = await call_next(request)
    if response.status_code == 401:
        ip = request.client.host if request.client else "unknown"
        now = _time.monotonic()
        window = _auth_fail_windows.setdefault(ip, _collections.deque())
        while window and now - window[0] > _AUTH_FAIL_WINDOW:
            window.popleft()
        window.append(now)
        if len(window) > _AUTH_FAIL_LIMIT:
            logger.warning(f"[auth] IP {ip} exceeded auth failure limit ({len(window)} failures/min) — blocking")
            if _PROM_ENABLED:
                _auth_failures_total.labels(ip).inc(len(window))
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many authentication failures. Try again later."},
                headers={"Retry-After": "60"},
            )
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": type(exc).__name__},
    )


app.add_middleware(_BodySizeLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cryptodataapi.dev", "https://www.cryptodataapi.dev"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["Content-Type", "X-API-Key", "X-Internal-Secret"],
)

# Routers
app.include_router(billing.router)
app.include_router(internal.router)
app.include_router(health.router)
app.include_router(symbols.router, prefix="/v1")
app.include_router(trades.router, prefix="/v1")
app.include_router(books.router, prefix="/v1")
app.include_router(funding.router, prefix="/v1")
app.include_router(metrics.router, prefix="/v1")
app.include_router(ohlcv.router, prefix="/v1")
app.include_router(open_interest.router, prefix="/v1")
app.include_router(mark_price.router, prefix="/v1")
app.include_router(liquidations.router, prefix="/v1")
app.include_router(intelligence.router)   # intelligence routes carry their own /v1 prefix


@app.get("/", include_in_schema=False)
def root():
    return {
        "name": "CryptoFeed API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    """Prometheus scrape endpoint — accessible only from the internal network (nginx blocks external)."""
    if not _PROM_ENABLED:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("# prometheus_client not installed\n", status_code=200)
    return _Response(_prom_generate(_PROM_REGISTRY), media_type=_PROM_CONTENT_TYPE)
