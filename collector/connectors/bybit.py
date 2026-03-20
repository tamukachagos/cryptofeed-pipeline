"""Bybit Linear Perpetuals WebSocket connector (V5 API).

Subscribes to:
  - orderbook.50.<SYMBOL>   — order book snapshots + deltas
  - publicTrade.<SYMBOL>    — public trade feed
  - kline.1.<SYMBOL>        — 1-minute OHLCV candles
  - liquidation.<SYMBOL>    — liquidation events
  - tickers.<SYMBOL>        — mark price, index price, open interest

Publishes to Redis streams:
  bybit:depth:<SYMBOL>
  bybit:trades:<SYMBOL>
  bybit:ohlcv:<SYMBOL>
  bybit:liquidations:<SYMBOL>
  bybit:mark_price:<SYMBOL>
  bybit:open_interest:<SYMBOL>
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
            args.append(f"kline.1.{sym}")
            args.append(f"liquidation.{sym}")
            args.append(f"tickers.{sym}")

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
        elif topic.startswith("kline."):
            symbol = topic.split(".")[-1]
            await self._handle_kline(symbol, data, ts_ns)
        elif topic.startswith("liquidation."):
            symbol = topic.split(".")[-1]
            await self._handle_liquidation(symbol, data, ts_ns)
        elif topic.startswith("tickers."):
            symbol = topic.split(".")[-1]
            await self._handle_tickers(symbol, data, ts_ns)

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
            applied = book.apply_delta(bids, asks, seq=seq)
            if not applied and book._gap_count > 0:
                # Gap detected — reset book state so next snapshot re-initializes
                book._initialized = False
                logger.warning(
                    f"[bybit] Seq gap on {symbol} (seq={seq}, last={book._last_seq}) "
                    f"— awaiting next snapshot for re-init"
                )

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
                    "is_liquidation": int(bool(trade.get("BT", False))),
                },
            )

    async def _handle_kline(self, symbol: str, data: list, ts_ns: int) -> None:
        for candle in (data if isinstance(data, list) else [data]):
            await self._publish(
                f"bybit:ohlcv:{symbol}",
                {
                    "exchange": "bybit",
                    "symbol": symbol,
                    "timestamp_ns": int(candle.get("start", 0)) * 1_000_000,
                    "interval": str(candle.get("interval", "1")),
                    "open": float(candle.get("open", 0)),
                    "high": float(candle.get("high", 0)),
                    "low": float(candle.get("low", 0)),
                    "close": float(candle.get("close", 0)),
                    "volume": float(candle.get("volume", 0)),
                    "quote_volume": float(candle.get("turnover", 0)),
                    "is_closed": int(bool(candle.get("confirm", False))),
                },
            )

    async def _handle_liquidation(self, symbol: str, data: dict, ts_ns: int) -> None:
        # Bybit liquidation: side is the position side
        raw_side = data.get("side", "Buy")
        side = "buy" if raw_side == "Buy" else "sell"
        await self._publish(
            f"bybit:liquidations:{symbol}",
            {
                "exchange": "bybit",
                "symbol": symbol,
                "timestamp_ns": int(data.get("updatedTime", 0)) * 1_000_000 or ts_ns,
                "side": side,
                "price": float(data.get("price", 0)),
                "qty": float(data.get("size", 0)),
            },
        )

    async def _handle_tickers(self, symbol: str, data: dict, ts_ns: int) -> None:
        mp = data.get("markPrice")
        ip = data.get("indexPrice")
        oi = data.get("openInterest")
        oiv = data.get("openInterestValue")

        if mp:
            await self._publish(
                f"bybit:mark_price:{symbol}",
                {
                    "exchange": "bybit",
                    "symbol": symbol,
                    "timestamp_ns": ts_ns,
                    "mark_price": float(mp),
                    "index_price": float(ip) if ip else "",
                },
            )

        if oi:
            await self._publish(
                f"bybit:open_interest:{symbol}",
                {
                    "exchange": "bybit",
                    "symbol": symbol,
                    "timestamp_ns": ts_ns,
                    "open_interest": float(oi),
                    "open_interest_value": float(oiv) if oiv else "",
                },
            )
