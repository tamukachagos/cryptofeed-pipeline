"""Funding rate endpoint."""
from __future__ import annotations

import os
import uuid
from datetime import timezone

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from api.auth import optional_auth, APIKeyInfo
from api.models import PaginatedResponse
from api.rate_limiter import check_rate_limit, check_daily_quota, record_row_usage, check_cursor_pattern
from api.routers.trades import _parse_dt, _enforce_plan_window, sys_path_fix

router = APIRouter(tags=["funding"])

_replay_engine = None


def _get_engine():
    global _replay_engine
    if _replay_engine is None:
        sys_path_fix()
        from replay.engine import ReplayEngine
        _replay_engine = ReplayEngine(data_dir=os.getenv("DATA_DIR", "./data"))
    return _replay_engine


@router.get("/funding")
async def get_funding(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    limit: int = Query(5_000, ge=1, le=50_000),
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
    df = engine._load("funding", exchange.lower(), symbol.upper(), start_dt, end_dt)

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

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

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
