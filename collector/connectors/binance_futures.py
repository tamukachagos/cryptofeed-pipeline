"""Binance USDT-M Futures WebSocket connector.

Subscribes to:
  - <symbol>@depth@100ms  — incremental order book updates
  - <symbol>@aggTrade     — aggregated trades
  - <symbol>@markPrice    — mark price + funding rate

Publishes to Redis streams:
  binance:depth:<SYMBOL>
  binance:trades:<SYMBOL>
  binance:funding:<SYMBOL>
"""
from __future__ import annotations

import asyncio
import json
import time

import redis.asyncio as aioredis
from loguru import logger

from collector.book_engine import OrderBookEngine
from collector.connectors.base import BaseConnector

_WS_BASE = "wss://fstream.binance.com/stream"
_PING_INTERVAL = 20  # seconds


class BinanceFuturesConnector(BaseConnector):
    exchange = "binance"

    def __init__(self, symbols: list[str], redis_client: aioredis.Redis, **kwargs) -> None:
        super().__init__(symbols, redis_client, **kwargs)
        self._books: dict[str, OrderBookEngine] = {
            s: OrderBookEngine(s, "binance") for s in symbols
        }

    @property
    def _ws_url(self) -> str:
        streams = []
        for sym in self.symbols:
            s = sym.lower()
            streams += [f"{s}@depth@100ms", f"{s}@aggTrade", f"{s}@markPrice@1s"]
        return f"{_WS_BASE}?streams=" + "/".join(streams)

    async def subscribe(self, ws) -> None:
        # Binance combined stream URL handles subscription automatically
        # Send a dummy message to confirm connection
        pass

    async def _recv_loop(self, ws) -> None:
        last_ping = time.monotonic()
        async for message in ws:
            await self._handle_message(message)
            # Send ping every 20s to keep connection alive
            if time.monotonic() - last_ping > _PING_INTERVAL:
                try:
                    await ws.ping()
                    last_ping = time.monotonic()
                except Exception:
                    pass

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        data = msg.get("data", {})
        event = data.get("e", "")
        symbol = data.get("s", "").upper()

        if not symbol:
            return

        if event == "depthUpdate":
            await self._handle_depth(symbol, data)
        elif event == "aggTrade":
            await self._handle_trade(symbol, data)
        elif event == "markPriceUpdate":
            await self._handle_funding(symbol, data)

    async def _handle_depth(self, symbol: str, data: dict) -> None:
        book = self._books.get(symbol)
        if book is None:
            return

        bids = data.get("b", [])
        asks = data.get("a", [])
        first_uid = int(data.get("U", 0))  # First update ID in this event
        last_uid = int(data.get("u", 0))   # Last update ID in this event

        if not book._initialized:
            # First message: treat as snapshot baseline
            book.apply_snapshot(bids, asks, seq=last_uid)
        else:
            # Apply delta without strict gap checking.
            # Binance combined stream buffers many msgs before first snapshot arrives;
            # using REST snapshot + buffer replay is complex and unnecessary for
            # microstructure data where we re-snapshot on reconnect anyway.
            book.apply_delta(bids, asks, seq=-1)
            book._last_seq = last_uid

        snap = book.snapshot(levels=20)
        await self._publish(
            f"binance:depth:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(data.get("E", 0)) * 1_000_000,
                "seq": last_uid,
                "bids": json.dumps(snap["bids"]),
                "asks": json.dumps(snap["asks"]),
                "spread": snap.get("spread") or "",
                "midprice": snap.get("midprice") or "",
                "microprice": snap.get("microprice") or "",
                "obi_5": snap.get("obi_5") or "",
            },
        )

    async def _handle_trade(self, symbol: str, data: dict) -> None:
        side = "sell" if data.get("m") else "buy"  # m=True means buyer is maker → seller aggressed
        await self._publish(
            f"binance:trades:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(data.get("T", 0)) * 1_000_000,
                "trade_id": str(data.get("l", "")),  # last trade ID in aggregate
                "price": float(data.get("p", 0)),
                "qty": float(data.get("q", 0)),
                "side": side,
                "is_liquidation": False,
            },
        )

    async def _handle_funding(self, symbol: str, data: dict) -> None:
        rate = data.get("r")
        if rate is None:
            return
        await self._publish(
            f"binance:funding:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(data.get("E", 0)) * 1_000_000,
                "funding_rate": float(rate),
                "next_funding_time_ns": int(data.get("T", 0)) * 1_000_000,
            },
        )
