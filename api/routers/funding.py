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
from api.rate_limiter import check_rate_limit
from api.routers.trades import _parse_dt, _enforce_plan_window, sys_path_fix

router = APIRouter(tags=["funding"])


@router.get("/funding")
def get_funding(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    limit: int = Query(5_000, ge=1, le=50_000),
    cursor: str | None = Query(None),
    key: APIKeyInfo = Depends(optional_auth),
):
    check_rate_limit(key)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")
    _enforce_plan_window(start_dt, end_dt, key)

    sys_path_fix()
    from replay.engine import ReplayEngine
    data_dir = os.getenv("DATA_DIR", "./data")
    engine = ReplayEngine(data_dir=data_dir)

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
