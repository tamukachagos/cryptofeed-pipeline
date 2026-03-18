"""Trades data endpoint — the core API product."""
from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from api.auth import optional_auth, APIKeyInfo
from api.models import TradeRecord, PaginatedResponse
from api.rate_limiter import check_rate_limit

router = APIRouter(tags=["trades"])

MAX_ROWS_PER_REQUEST = 50_000


def _parse_dt(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid datetime: {s}. Use ISO 8601 (e.g. 2026-03-17T00:00:00Z)")


def _enforce_plan_window(start: datetime, end: datetime, key: APIKeyInfo) -> None:
    delta_days = (end - start).total_seconds() / 86400
    if delta_days > key.max_days:
        raise HTTPException(
            status_code=403,
            detail=f"Your {key.plan} plan allows max {key.max_days} days per request. "
                   f"Requested {delta_days:.1f} days. Upgrade at cryptofeed.io/pricing"
        )


@router.get("/trades")
def get_trades(
    exchange: str = Query(..., description="Exchange name: binance, bybit, okx"),
    symbol: str = Query(..., description="Symbol e.g. BTCUSDT"),
    start: str = Query(..., description="Start time ISO 8601 e.g. 2026-03-17T00:00:00Z"),
    end: str = Query(..., description="End time ISO 8601"),
    format: str = Query("json", description="Response format: json, csv, parquet"),
    limit: int = Query(10_000, ge=1, le=MAX_ROWS_PER_REQUEST),
    cursor: str | None = Query(None, description="Pagination cursor from previous response"),
    key: APIKeyInfo = Depends(optional_auth),
):
    check_rate_limit(key)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")
    _enforce_plan_window(start_dt, end_dt, key)

    # Load from Parquet
    sys_path_fix()
    from replay.engine import ReplayEngine
    data_dir = os.getenv("DATA_DIR", "./data")
    engine = ReplayEngine(data_dir=data_dir)

    df = engine._load("trades", exchange.lower(), symbol.upper(), start_dt, end_dt)

    if df.empty:
        if format == "json":
            return PaginatedResponse(
                data=[], count=0, next_cursor=None,
                exchange=exchange, symbol=symbol, start=start, end=end
            )
        return Response(content="", media_type="text/csv")

    # Apply cursor (offset by timestamp_ns)
    if cursor:
        try:
            cursor_ns = int(cursor)
            df = df[df["timestamp_ns"] > cursor_ns]
        except ValueError:
            pass

    total = len(df)
    df = df.head(limit)
    next_cursor = str(int(df["timestamp_ns"].iloc[-1])) if len(df) == limit and total > limit else None

    if format == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="zstd")
        buf.seek(0)
        filename = f"trades_{exchange}_{symbol}_{start[:10]}_{end[:10]}.parquet"
        return StreamingResponse(
            buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}",
                     "X-Request-ID": str(uuid.uuid4())},
        )

    if format == "csv":
        csv_buf = io.StringIO()
        df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        df.to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        filename = f"trades_{exchange}_{symbol}_{start[:10]}_{end[:10]}.csv"
        return StreamingResponse(
            io.BytesIO(csv_buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}",
                     "X-Request-ID": str(uuid.uuid4())},
        )

    # JSON (default)
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    records = df.to_dict(orient="records")

    return PaginatedResponse(
        data=records,
        count=len(records),
        total_available=total,
        next_cursor=next_cursor,
        exchange=exchange,
        symbol=symbol,
        start=start,
        end=end,
    )


def sys_path_fix():
    import sys
    from pathlib import Path
    root = str(Path(__file__).resolve().parents[2])
    if root not in sys.path:
        sys.path.insert(0, root)
