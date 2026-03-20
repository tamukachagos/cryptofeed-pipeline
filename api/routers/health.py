"""Health check endpoint."""
from __future__ import annotations

import os
from pathlib import Path

import redis
from fastapi import APIRouter

from api.models import HealthResponse

router = APIRouter(tags=["health"])

STREAMS_TO_CHECK = [
    "binance:trades:BTCUSDT", "bybit:trades:BTCUSDT", "okx:trades:BTCUSDT",
    "binance:depth:BTCUSDT", "bybit:depth:BTCUSDT",
]


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    stream_lengths = {}
    try:
        password = os.getenv("REDIS_PASSWORD", "") or None
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=password,
            decode_responses=True,
            socket_timeout=2,
        )
        for s in STREAMS_TO_CHECK:
            stream_lengths[s] = r.xlen(s)
    except Exception:
        stream_lengths = {"redis": -1}

    # Approximate data dir size
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    total_bytes = sum(f.stat().st_size for f in data_dir.rglob("*.parquet")) if data_dir.exists() else 0

    return HealthResponse(
        status="ok",
        streams=stream_lengths,
        data_dir_gb=round(total_bytes / 1e9, 3),
    )
