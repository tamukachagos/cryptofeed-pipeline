"""Order book snapshots endpoint."""
from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from api.auth import optional_auth, APIKeyInfo
from api.models import PaginatedResponse
from api.rate_limiter import check_rate_limit, check_daily_quota, record_row_usage, check_cursor_pattern
from api.routers.trades import _parse_dt, _enforce_plan_window, sys_path_fix

router = APIRouter(tags=["books"])

_replay_engine = None


def _get_engine():
    global _replay_engine
    if _replay_engine is None:
        from api.routers.trades import sys_path_fix
        sys_path_fix()
        from replay.engine import ReplayEngine
        _replay_engine = ReplayEngine(data_dir=os.getenv("DATA_DIR", "./data"))
    return _replay_engine


@router.get("/books")
async def get_books(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    levels: int = Query(20, ge=1, le=400, description="Number of price levels to return"),
    format: str = Query("json"),
    limit: int = Query(1_000, ge=1, le=10_000),
    cursor: str | None = Query(None),
    key: APIKeyInfo = Depends(optional_auth),
):
    await check_rate_limit(key)
    await check_daily_quota(key)
    await check_cursor_pattern(key, cursor)
    from api.routers.trades import _validate_path_params
    _validate_path_params(exchange, symbol)

    # Enforce plan level cap
    levels = min(levels, key.max_levels)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")
    _enforce_plan_window(start_dt, end_dt, key)
    limit = min(limit, key.max_rows)

    engine = _get_engine()
    df = engine._load("books", exchange.lower(), symbol.upper(), start_dt, end_dt)

    if df.empty:
        return PaginatedResponse(data=[], count=0, next_cursor=None,
                                 exchange=exchange, symbol=symbol, start=start, end=end)

    if cursor:
        try:
            df = df[df["timestamp_ns"] > int(cursor)]
        except ValueError:
            pass

    total = len(df)
    df = df.head(limit)
    await record_row_usage(key, len(df))
    next_cursor = str(int(df["timestamp_ns"].iloc[-1])) if len(df) == limit and total > limit else None

    # Trim bids/asks to requested levels
    def trim_levels(val, n):
        if isinstance(val, str):
            try:
                data = json.loads(val)
                return data[:n]
            except Exception:
                return []
        if isinstance(val, list):
            return val[:n]
        return []

    df = df.copy()
    df["bids"] = df["bids"].apply(lambda v: trim_levels(v, levels))
    df["asks"] = df["asks"].apply(lambda v: trim_levels(v, levels))
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if format == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="zstd")
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/octet-stream",
                                 headers={"Content-Disposition": f"attachment; filename=books_{exchange}_{symbol}.parquet",
                                          "X-Request-ID": str(uuid.uuid4())})

    if format == "csv":
        csv_out = df.drop(columns=["bids", "asks"], errors="ignore").to_csv(index=False)
        return StreamingResponse(io.BytesIO(csv_out.encode()), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename=books_{exchange}_{symbol}.csv"})

    records = df.to_dict(orient="records")
    return PaginatedResponse(data=records, count=len(records), total_available=total,
                             next_cursor=next_cursor, exchange=exchange, symbol=symbol,
                             start=start, end=end)
