"""Bybit Linear Perpetuals WebSocket connector (V5 API).

Subscribes to:
  - orderbook.50.<SYMBOL>  — order book snapshots + deltas
  - publicTrade.<SYMBOL>   — public trade feed

Publishes to Redis streams:
  bybit:depth:<SYMBOL>
  bybit:trades:<SYMBOL>
"""
from __future__ import annotations

import asyncio
import json
import time

import redis.asyncio as aioredis
from loguru import logger

from collector.book_engine import OrderBookEngine
from collector.connectors.base import BaseConnector

_WS_URL = "wss://stream.bybit.com/v5/public/linear"
_PING_INTERVAL = 20  # seconds


class BybitConnector(BaseConnector):
    exchange = "bybit"

    def __init__(self, symbols: list[str], redis_client: aioredis.Redis, **kwargs) -> None:
        super().__init__(symbols, redis_client, **kwargs)
        self._books: dict[str, OrderBookEngine] = {
            s: OrderBookEngine(s, "bybit") for s in symbols
        }

    @property
    def _ws_url(self) -> str:
        return _WS_URL

    async def subscribe(self, ws) -> None:
        args = []
        for sym in self.symbols:
            args.append(f"orderbook.50.{sym}")
            args.append(f"publicTrade.{sym}")

        sub_msg = json.dumps({"op": "subscribe", "args": args})
        await ws.send(sub_msg)
        logger.info(f"[bybit] Subscribed to {args}")

    async def _recv_loop(self, ws) -> None:
        last_ping = time.monotonic()
        async for message in ws:
            await self._handle_message(message)
            if time.monotonic() - last_ping > _PING_INTERVAL:
                try:
                    await ws.send(json.dumps({"op": "ping"}))
                    last_ping = time.monotonic()
                except Exception:
                    pass

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Ignore heartbeat/subscription confirmations
        op = msg.get("op", "")
        if op in ("pong", "subscribe"):
            return

        topic = msg.get("topic", "")
        if not topic:
            return

        ts_ns = int(msg.get("ts", 0)) * 1_000_000
        data = msg.get("data", {})
        msg_type = msg.get("type", "snapshot")

        if topic.startswith("orderbook."):
            symbol = topic.split(".")[-1]
            await self._handle_depth(symbol, data, msg_type, ts_ns)
        elif topic.startswith("publicTrade."):
            symbol = topic.split(".")[-1]
            await self._handle_trades(symbol, data, ts_ns)

    async def _handle_depth(self, symbol: str, data: dict, msg_type: str, ts_ns: int) -> None:
        book = self._books.get(symbol)
        if book is None:
            return

        bids = data.get("b", [])
        asks = data.get("a", [])
        seq = int(data.get("seq", -1))

        if msg_type == "snapshot":
            book.apply_snapshot(bids, asks, seq)
        else:
            # Pass seq=-1 to skip strict gap check; Bybit seq doesn't increment by +1
            book.apply_delta(bids, asks, seq=-1)
            book._last_seq = seq

        snap = book.snapshot(levels=20)
        await self._publish(
            f"bybit:depth:{symbol}",
            {
                "exchange": "bybit",
                "symbol": symbol,
                "timestamp_ns": ts_ns,
                "seq": seq,
                "bids": json.dumps(snap["bids"]),
                "asks": json.dumps(snap["asks"]),
                "spread": book.spread() or "",
                "midprice": book.midprice() or "",
                "microprice": book.microprice() or "",
                "obi_5": book.order_book_imbalance(5) or "",
            },
        )

    async def _handle_trades(self, symbol: str, trades: list, ts_ns: int) -> None:
        for trade in trades:
            side = "buy" if trade.get("S") == "Buy" else "sell"
            await self._publish(
                f"bybit:trades:{symbol}",
                {
                    "exchange": "bybit",
                    "symbol": symbol,
                    "timestamp_ns": int(trade.get("T", 0)) * 1_000_000,
                    "trade_id": str(trade.get("i", "")),
                    "price": float(trade.get("p", 0)),
                    "qty": float(trade.get("v", 0)),
                    "side": side,
                    "is_liquidation": bool(trade.get("BT", False)),
                },
            )
