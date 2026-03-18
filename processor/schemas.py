"""Normalized message schemas (Pydantic v2)."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class NormalizedTrade(BaseModel):
    exchange: str
    symbol: str           # Normalized: BTCUSDT
    timestamp_ns: int     # Nanoseconds since epoch
    trade_id: str
    price: float
    qty: float
    side: Literal["buy", "sell"]
    is_liquidation: bool = False


class NormalizedBookSnapshot(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    seq: int
    bids: list[list[float]]   # [[price, qty], ...]
    asks: list[list[float]]
    spread: float | None = None
    midprice: float | None = None
    microprice: float | None = None
    obi_5: float | None = None   # Order book imbalance, top 5 levels


class NormalizedFunding(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    funding_rate: float
    next_funding_time_ns: int | None = None


class NormalizedBookDelta(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    seq: int
    bids: list[list[float]]
    asks: list[list[float]]
