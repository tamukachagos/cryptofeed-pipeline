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


class NormalizedOHLCV(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int     # Candle open time in nanoseconds
    interval: str         # "1m", "5m", "1h", etc.
    open: float
    high: float
    low: float
    close: float
    volume: float         # Base asset volume
    quote_volume: float   # Quote asset volume (USDT)
    trades: int | None = None
    is_closed: bool = False


class NormalizedOpenInterest(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    open_interest: float        # In base contracts
    open_interest_value: float | None = None  # In USD


class NormalizedMarkPrice(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    mark_price: float
    index_price: float | None = None


class NormalizedLiquidation(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    side: Literal["buy", "sell"]   # Side of the liquidated position
    price: float
    qty: float
    order_type: str | None = None  # "limit" or "market"
