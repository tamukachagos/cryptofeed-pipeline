"""Pydantic response models for all API endpoints."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TradeRecord(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    timestamp: str
    price: float
    qty: float
    side: str
    is_liquidation: bool = False


class BookRecord(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    timestamp: str
    seq: int
    bids: list[list[float]]
    asks: list[list[float]]
    spread: float | None = None
    midprice: float | None = None
    microprice: float | None = None
    obi_5: float | None = None


class FundingRecord(BaseModel):
    exchange: str
    symbol: str
    timestamp_ns: int
    timestamp: str
    funding_rate: float
    next_funding_time_ns: int | None = None


class PaginatedResponse(BaseModel):
    data: list[Any]
    count: int
    total_available: int | None = None
    next_cursor: str | None = None
    exchange: str
    symbol: str
    start: str
    end: str


class SymbolInfo(BaseModel):
    exchange: str
    symbol: str
    data_types: list[str]
    earliest_date: str | None = None
    latest_date: str | None = None
    trade_count_approx: int | None = None


class HealthResponse(BaseModel):
    status: str
    version: str = "1.0.0"
    streams: dict[str, int] = Field(default_factory=dict)
    data_dir_gb: float | None = None


class MetricRecord(BaseModel):
    timestamp_ns: int
    timestamp: str
    value: float
