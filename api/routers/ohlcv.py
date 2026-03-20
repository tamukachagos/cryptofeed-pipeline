"""OHLCV (candlestick) data endpoint."""
from __future__ import annotations

import io
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from api.auth import optional_auth, APIKeyInfo
from api.models import PaginatedResponse
from api.rate_limiter import check_rate_limit, check_daily_quota, record_row_usage, check_cursor_pattern

router = APIRouter(tags=["ohlcv"])

MAX_ROWS_PER_REQUEST = 100_000

_replay_engine = None


def _get_engine():
    global _replay_engine
    if _replay_engine is None:
        _sys_path_fix()
        from replay.engine import ReplayEngine
        _replay_engine = ReplayEngine(data_dir=os.getenv("DATA_DIR", "./data"))
    return _replay_engine


def _parse_dt(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid datetime: {s}. Use ISO 8601.")


def _enforce_plan_window(start: datetime, end: datetime, key: APIKeyInfo) -> None:
    delta_days = (end - start).total_seconds() / 86400
    if delta_days > key.max_days:
        raise HTTPException(
            status_code=403,
            detail=f"Your {key.plan} plan allows max {key.max_days} days per request."
        )


@router.get("/ohlcv")
async def get_ohlcv(
    exchange: str = Query(..., description="Exchange: binance, bybit, okx"),
    symbol: str = Query(..., description="Symbol e.g. BTCUSDT"),
    start: str = Query(..., description="Start time ISO 8601"),
    end: str = Query(..., description="End time ISO 8601"),
    interval: str = Query("1m", description="Candle interval: 1m"),
    format: str = Query("json", description="json, csv, or parquet"),
    limit: int = Query(10_000, ge=1, le=MAX_ROWS_PER_REQUEST),
    cursor: str | None = Query(None),
    key: APIKeyInfo = Depends(optional_auth),
):
    await check_rate_limit(key)
    await check_daily_quota(key)
    await check_cursor_pattern(key, cursor)
    from api.routers.trades import _validate_path_params
    _validate_path_params(exchange, symbol)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")
    _enforce_plan_window(start_dt, end_dt, key)
    limit = min(limit, key.max_rows)

    engine = _get_engine()
    df = engine._load("ohlcv", exchange.lower(), symbol.upper(), start_dt, end_dt)

    if not df.empty and interval != "1m":
        df = df[df["interval"] == interval]

    if df.empty:
        if format == "json":
            return PaginatedResponse(data=[], count=0, next_cursor=None,
                                     exchange=exchange, symbol=symbol, start=start, end=end)
        return Response(content="", media_type="text/csv")

    if cursor:
        try:
            df = df[df["timestamp_ns"] > int(cursor)]
        except ValueError:
            pass

    total = len(df)
    df = df.head(limit)
    await record_row_usage(key, len(df))
    next_cursor = str(int(df["timestamp_ns"].iloc[-1])) if len(df) == limit and total > limit else None

    if format == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="zstd")
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/octet-stream",
                                 headers={"Content-Disposition": f"attachment; filename=ohlcv_{exchange}_{symbol}_{start[:10]}_{end[:10]}.parquet",
                                          "X-Request-ID": str(uuid.uuid4())})

    if format == "csv":
        df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(io.BytesIO(buf.getvalue().encode()), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename=ohlcv_{exchange}_{symbol}_{start[:10]}_{end[:10]}.csv",
                                          "X-Request-ID": str(uuid.uuid4())})

    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return PaginatedResponse(
        data=df.to_dict(orient="records"),
        count=len(df),
        total_available=total,
        next_cursor=next_cursor,
        exchange=exchange,
        symbol=symbol,
        start=start,
        end=end,
    )


def _sys_path_fix():
    import sys
    from pathlib import Path
    root = str(Path(__file__).resolve().parents[2])
    if root not in sys.path:
        sys.path.insert(0, root)
