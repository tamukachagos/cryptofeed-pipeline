"""Per-exchange normalization: raw Redis dict → NormalizedMessage."""
from __future__ import annotations

import json

from processor.schemas import (
    NormalizedBookSnapshot,
    NormalizedFunding,
    NormalizedLiquidation,
    NormalizedMarkPrice,
    NormalizedOHLCV,
    NormalizedOpenInterest,
    NormalizedTrade,
)


# ---------------------------------------------------------------------------
# Symbol normalization helpers
# ---------------------------------------------------------------------------

def normalize_symbol(raw: str) -> str:
    """Convert any exchange-specific symbol to standard BTCUSDT form.

    Examples:
        BTC-USDT-SWAP → BTCUSDT
        BTC_USDT      → BTCUSDT
        BTCUSDT       → BTCUSDT
    """
    s = raw.upper()
    s = s.replace("-USDT-SWAP", "USDT")
    s = s.replace("_USDT", "USDT")
    s = s.replace("-", "")
    return s


# ---------------------------------------------------------------------------
# Binance
# ---------------------------------------------------------------------------

def normalize_binance_trade(raw: dict) -> NormalizedTrade:
    return NormalizedTrade(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        trade_id=str(raw.get("trade_id", "")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        side=str(raw.get("side", "buy")),
        is_liquidation=_parse_bool(raw.get("is_liquidation", False)),
    )


def normalize_binance_funding(raw: dict) -> NormalizedFunding:
    nft = raw.get("next_funding_time_ns")
    return NormalizedFunding(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        funding_rate=float(raw.get("funding_rate", 0)),
        next_funding_time_ns=int(nft) if nft and nft != "" else None,
    )


def normalize_binance_book(raw: dict) -> NormalizedBookSnapshot:
    return _parse_book_snapshot(raw, "binance")


def normalize_binance_ohlcv(raw: dict) -> NormalizedOHLCV:
    return NormalizedOHLCV(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        interval=str(raw.get("interval", "1m")),
        open=float(raw.get("open", 0)),
        high=float(raw.get("high", 0)),
        low=float(raw.get("low", 0)),
        close=float(raw.get("close", 0)),
        volume=float(raw.get("volume", 0)),
        quote_volume=float(raw.get("quote_volume", 0)),
        trades=int(raw["trades"]) if raw.get("trades") and raw["trades"] != "" else None,
        is_closed=_parse_bool(raw.get("is_closed", False)),
    )


def normalize_binance_oi(raw: dict) -> NormalizedOpenInterest:
    oiv = raw.get("open_interest_value")
    return NormalizedOpenInterest(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        open_interest=float(raw.get("open_interest", 0)),
        open_interest_value=float(oiv) if oiv and oiv != "" else None,
    )


def normalize_binance_mark_price(raw: dict) -> NormalizedMarkPrice:
    ip = raw.get("index_price")
    return NormalizedMarkPrice(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        mark_price=float(raw.get("mark_price", 0)),
        index_price=float(ip) if ip and ip != "" else None,
    )


def normalize_binance_liquidation(raw: dict) -> NormalizedLiquidation:
    return NormalizedLiquidation(
        exchange="binance",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        side=str(raw.get("side", "buy")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        order_type=str(raw["order_type"]) if raw.get("order_type") else None,
    )


# ---------------------------------------------------------------------------
# Bybit
# ---------------------------------------------------------------------------

def normalize_bybit_trade(raw: dict) -> NormalizedTrade:
    return NormalizedTrade(
        exchange="bybit",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        trade_id=str(raw.get("trade_id", "")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        side=str(raw.get("side", "buy")),
        is_liquidation=_parse_bool(raw.get("is_liquidation", False)),
    )


def normalize_bybit_book(raw: dict) -> NormalizedBookSnapshot:
    return _parse_book_snapshot(raw, "bybit")


def normalize_bybit_ohlcv(raw: dict) -> NormalizedOHLCV:
    return NormalizedOHLCV(
        exchange="bybit",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        interval=str(raw.get("interval", "1")),
        open=float(raw.get("open", 0)),
        high=float(raw.get("high", 0)),
        low=float(raw.get("low", 0)),
        close=float(raw.get("close", 0)),
        volume=float(raw.get("volume", 0)),
        quote_volume=float(raw.get("quote_volume", 0)),
        trades=None,
        is_closed=_parse_bool(raw.get("is_closed", False)),
    )


def normalize_bybit_oi(raw: dict) -> NormalizedOpenInterest:
    oiv = raw.get("open_interest_value")
    return NormalizedOpenInterest(
        exchange="bybit",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        open_interest=float(raw.get("open_interest", 0)),
        open_interest_value=float(oiv) if oiv and oiv != "" else None,
    )


def normalize_bybit_mark_price(raw: dict) -> NormalizedMarkPrice:
    ip = raw.get("index_price")
    return NormalizedMarkPrice(
        exchange="bybit",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        mark_price=float(raw.get("mark_price", 0)),
        index_price=float(ip) if ip and ip != "" else None,
    )


def normalize_bybit_liquidation(raw: dict) -> NormalizedLiquidation:
    return NormalizedLiquidation(
        exchange="bybit",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        side=str(raw.get("side", "buy")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        order_type=None,
    )


# ---------------------------------------------------------------------------
# OKX
# ---------------------------------------------------------------------------

def normalize_okx_trade(raw: dict) -> NormalizedTrade:
    return NormalizedTrade(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        trade_id=str(raw.get("trade_id", "")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        side=str(raw.get("side", "buy")),
        is_liquidation=False,
    )


def normalize_okx_funding(raw: dict) -> NormalizedFunding:
    nft = raw.get("next_funding_time_ns")
    return NormalizedFunding(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        funding_rate=float(raw.get("funding_rate", 0)),
        next_funding_time_ns=int(nft) if nft and nft != "" else None,
    )


def normalize_okx_book(raw: dict) -> NormalizedBookSnapshot:
    return _parse_book_snapshot(raw, "okx")


def normalize_okx_ohlcv(raw: dict) -> NormalizedOHLCV:
    return NormalizedOHLCV(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        interval=str(raw.get("interval", "1m")),
        open=float(raw.get("open", 0)),
        high=float(raw.get("high", 0)),
        low=float(raw.get("low", 0)),
        close=float(raw.get("close", 0)),
        volume=float(raw.get("volume", 0)),
        quote_volume=float(raw.get("quote_volume", 0)),
        trades=int(raw["trades"]) if raw.get("trades") and raw["trades"] != "" else None,
        is_closed=_parse_bool(raw.get("is_closed", False)),
    )


def normalize_okx_oi(raw: dict) -> NormalizedOpenInterest:
    oiv = raw.get("open_interest_value")
    return NormalizedOpenInterest(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        open_interest=float(raw.get("open_interest", 0)),
        open_interest_value=float(oiv) if oiv and oiv != "" else None,
    )


def normalize_okx_mark_price(raw: dict) -> NormalizedMarkPrice:
    ip = raw.get("index_price")
    return NormalizedMarkPrice(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        mark_price=float(raw.get("mark_price", 0)),
        index_price=float(ip) if ip and ip != "" else None,
    )


def normalize_okx_liquidation(raw: dict) -> NormalizedLiquidation:
    return NormalizedLiquidation(
        exchange="okx",
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        side=str(raw.get("side", "buy")),
        price=float(raw.get("price", 0)),
        qty=float(raw.get("qty", 0)),
        order_type=str(raw["order_type"]) if raw.get("order_type") else None,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_book_snapshot(raw: dict, exchange: str) -> NormalizedBookSnapshot:
    bids = raw.get("bids", "[]")
    asks = raw.get("asks", "[]")
    if isinstance(bids, str):
        bids = json.loads(bids)
    if isinstance(asks, str):
        asks = json.loads(asks)

    spread = raw.get("spread")
    midprice = raw.get("midprice")
    microprice = raw.get("microprice")
    obi_5 = raw.get("obi_5")

    return NormalizedBookSnapshot(
        exchange=exchange,
        symbol=normalize_symbol(str(raw.get("symbol", ""))),
        timestamp_ns=int(raw.get("timestamp_ns", 0)),
        seq=int(raw.get("seq", -1)),
        bids=[[float(p), float(q)] for p, q in bids],
        asks=[[float(p), float(q)] for p, q in asks],
        spread=float(spread) if spread and spread != "" else None,
        midprice=float(midprice) if midprice and midprice != "" else None,
        microprice=float(microprice) if microprice and microprice != "" else None,
        obi_5=float(obi_5) if obi_5 and obi_5 != "" else None,
    )


def _parse_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


# ---------------------------------------------------------------------------
# Dispatch tables
# ---------------------------------------------------------------------------

TRADE_NORMALIZERS = {
    "binance": normalize_binance_trade,
    "bybit": normalize_bybit_trade,
    "okx": normalize_okx_trade,
}

BOOK_NORMALIZERS = {
    "binance": normalize_binance_book,
    "bybit": normalize_bybit_book,
    "okx": normalize_okx_book,
}

FUNDING_NORMALIZERS = {
    "binance": normalize_binance_funding,
    "okx": normalize_okx_funding,
}

OHLCV_NORMALIZERS = {
    "binance": normalize_binance_ohlcv,
    "bybit": normalize_bybit_ohlcv,
    "okx": normalize_okx_ohlcv,
}

OI_NORMALIZERS = {
    "binance": normalize_binance_oi,
    "bybit": normalize_bybit_oi,
    "okx": normalize_okx_oi,
}

MARK_PRICE_NORMALIZERS = {
    "binance": normalize_binance_mark_price,
    "bybit": normalize_bybit_mark_price,
    "okx": normalize_okx_mark_price,
}

LIQUIDATION_NORMALIZERS = {
    "binance": normalize_binance_liquidation,
    "bybit": normalize_bybit_liquidation,
    "okx": normalize_okx_liquidation,
}
