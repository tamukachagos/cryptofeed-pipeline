"""On-the-fly microstructure metrics endpoint."""
from __future__ import annotations

import os
from datetime import timezone

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import optional_auth, APIKeyInfo
from api.models import MetricRecord, PaginatedResponse
from api.rate_limiter import check_rate_limit
from api.routers.trades import _parse_dt, _enforce_plan_window, sys_path_fix

router = APIRouter(tags=["metrics"])


def _load_trades(exchange, symbol, start_dt, end_dt) -> pd.DataFrame:
    sys_path_fix()
    from replay.engine import ReplayEngine
    data_dir = os.getenv("DATA_DIR", "./data")
    return ReplayEngine(data_dir=data_dir)._load("trades", exchange.lower(), symbol.upper(), start_dt, end_dt)


@router.get("/metrics/trade-imbalance")
def trade_imbalance(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    window: int = Query(100, ge=10, le=10_000, description="Rolling window in trades"),
    key: APIKeyInfo = Depends(optional_auth),
):
    """Rolling buy/sell volume imbalance. 1.0 = all buys, 0.0 = all sells."""
    check_rate_limit(key)
    start_dt, end_dt = _parse_dt(start), _parse_dt(end)
    _enforce_plan_window(start_dt, end_dt, key)

    sys_path_fix()
    from features.microstructure import compute_trade_imbalance
    df = _load_trades(exchange, symbol, start_dt, end_dt)
    if df.empty:
        return PaginatedResponse(data=[], count=0, next_cursor=None,
                                 exchange=exchange, symbol=symbol, start=start, end=end)

    imbalance = compute_trade_imbalance(df, window=window)
    df["value"] = imbalance
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    result = df[["timestamp_ns", "timestamp", "value"]].dropna().to_dict(orient="records")

    return PaginatedResponse(data=result, count=len(result), next_cursor=None,
                             exchange=exchange, symbol=symbol, start=start, end=end)


@router.get("/metrics/obi")
def order_book_imbalance(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    levels: int = Query(5, ge=1, le=20),
    key: APIKeyInfo = Depends(optional_auth),
):
    """Order book imbalance from book snapshots. Range [-1, 1]."""
    check_rate_limit(key)
    start_dt, end_dt = _parse_dt(start), _parse_dt(end)
    _enforce_plan_window(start_dt, end_dt, key)

    sys_path_fix()
    from features.microstructure import compute_obi
    from replay.engine import ReplayEngine
    data_dir = os.getenv("DATA_DIR", "./data")
    df = ReplayEngine(data_dir=data_dir)._load("books", exchange.lower(), symbol.upper(), start_dt, end_dt)

    if df.empty:
        return PaginatedResponse(data=[], count=0, next_cursor=None,
                                 exchange=exchange, symbol=symbol, start=start, end=end)

    df["value"] = compute_obi(df, levels=levels)
    df["timestamp"] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    result = df[["timestamp_ns", "timestamp", "value"]].dropna().to_dict(orient="records")

    return PaginatedResponse(data=result, count=len(result), next_cursor=None,
                             exchange=exchange, symbol=symbol, start=start, end=end)


@router.get("/metrics/vpin")
def vpin(
    exchange: str = Query(...),
    symbol: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    bucket_size: float = Query(0.1, ge=0.01, le=0.5, description="Fraction of total volume per bucket"),
    key: APIKeyInfo = Depends(optional_auth),
):
    """Volume-synchronized PIN (toxicity proxy). Higher = more toxic order flow."""
    check_rate_limit(key)
    start_dt, end_dt = _parse_dt(start), _parse_dt(end)
    _enforce_plan_window(start_dt, end_dt, key)

    sys_path_fix()
    from features.microstructure import compute_vpin
    df = _load_trades(exchange, symbol, start_dt, end_dt)
    if df.empty:
        return PaginatedResponse(data=[], count=0, next_cursor=None,
                                 exchange=exchange, symbol=symbol, start=start, end=end)

    vpin_series = compute_vpin(df, bucket_size=bucket_size)
    result_df = df.iloc[vpin_series.index][["timestamp_ns"]].copy()
    result_df["value"] = vpin_series.values
    result_df["timestamp"] = pd.to_datetime(result_df["timestamp_ns"], unit="ns", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    result = result_df.to_dict(orient="records")

    return PaginatedResponse(data=result, count=len(result), next_cursor=None,
                             exchange=exchange, symbol=symbol, start=start, end=end)
