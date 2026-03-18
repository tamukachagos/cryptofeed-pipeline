"""OKX Futures WebSocket connector (V5 API).

Subscribes to:
  - books          — order book (L2, 400 levels)
  - trades         — public trade feed
  - funding-rate   — funding rate updates

Symbol conversion: BTCUSDT → BTC-USDT-SWAP

Publishes to Redis streams:
  okx:depth:<SYMBOL>    (normalized symbol e.g. BTCUSDT)
  okx:trades:<SYMBOL>
  okx:funding:<SYMBOL>
"""
from __future__ import annotations

import asyncio
import json
import time

import redis.asyncio as aioredis
from loguru import logger

from collector.book_engine import OrderBookEngine
from collector.connectors.base import BaseConnector

_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
_PING_INTERVAL = 25  # seconds — OKX disconnects after 30s of silence


def _to_inst_id(symbol: str) -> str:
    """BTCUSDT → BTC-USDT-SWAP"""
    base = symbol.upper().replace("USDT", "")
    return f"{base}-USDT-SWAP"


def _from_inst_id(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT"""
    return inst_id.replace("-USDT-SWAP", "").replace("-", "") + "USDT"


class OKXConnector(BaseConnector):
    exchange = "okx"

    def __init__(self, symbols: list[str], redis_client: aioredis.Redis, **kwargs) -> None:
        super().__init__(symbols, redis_client, **kwargs)
        self._books: dict[str, OrderBookEngine] = {
            s: OrderBookEngine(s, "okx") for s in symbols
        }
        # Map inst_id → normalized symbol
        self._inst_to_sym = {_to_inst_id(s): s for s in symbols}

    @property
    def _ws_url(self) -> str:
        return _WS_URL

    async def subscribe(self, ws) -> None:
        args = []
        for sym in self.symbols:
            inst = _to_inst_id(sym)
            args += [
                {"channel": "books", "instId": inst},
                {"channel": "trades", "instId": inst},
                {"channel": "funding-rate", "instId": inst},
            ]
        sub_msg = json.dumps({"op": "subscribe", "args": args})
        await ws.send(sub_msg)
        logger.info(f"[okx] Subscribed for {self.symbols}")

    async def _recv_loop(self, ws) -> None:
        last_ping = time.monotonic()
        async for message in ws:
            # OKX ping is a plain "ping" string, pong is "pong"
            if message == "pong":
                continue
            await self._handle_message(message)
            if time.monotonic() - last_ping > _PING_INTERVAL:
                try:
                    await ws.send("ping")
                    last_ping = time.monotonic()
                except Exception:
                    pass

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Ignore subscription confirmations and errors
        if "event" in msg:
            return

        channel = msg.get("arg", {}).get("channel", "")
        inst_id = msg.get("arg", {}).get("instId", "")
        symbol = self._inst_to_sym.get(inst_id)
        if symbol is None:
            return

        action = msg.get("action", "snapshot")
        data_list = msg.get("data", [])
        if not data_list:
            return

        if channel == "books":
            await self._handle_depth(symbol, data_list[0], action)
        elif channel == "trades":
            await self._handle_trades(symbol, data_list)
        elif channel == "funding-rate":
            await self._handle_funding(symbol, data_list[0])

    async def _handle_depth(self, symbol: str, data: dict, action: str) -> None:
        book = self._books.get(symbol)
        if book is None:
            return

        bids = data.get("bids", [])
        asks = data.get("asks", [])
        seq = int(data.get("seqId", -1))
        ts_ns = int(data.get("ts", 0)) * 1_000_000

        if action == "snapshot":
            book.apply_snapshot(bids, asks, seq)
        else:
            # Pass seq=-1 to skip strict gap check; OKX seqId doesn't increment by +1
            book.apply_delta(bids, asks, seq=-1)
            book._last_seq = seq

        snap = book.snapshot(levels=20)
        await self._publish(
            f"okx:depth:{symbol}",
            {
                "exchange": "okx",
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

    async def _handle_trades(self, symbol: str, trades: list) -> None:
        for trade in trades:
            side = trade.get("side", "buy").lower()
            await self._publish(
                f"okx:trades:{symbol}",
                {
                    "exchange": "okx",
                    "symbol": symbol,
                    "timestamp_ns": int(trade.get("ts", 0)) * 1_000_000,
                    "trade_id": str(trade.get("tradeId", "")),
                    "price": float(trade.get("px", 0)),
                    "qty": float(trade.get("sz", 0)),
                    "side": side,
                    "is_liquidation": False,
                },
            )

    async def _handle_funding(self, symbol: str, data: dict) -> None:
        rate = data.get("fundingRate")
        if rate is None:
            return
        next_ts = data.get("nextFundingTime")
        await self._publish(
            f"okx:funding:{symbol}",
            {
                "exchange": "okx",
                "symbol": symbol,
                "timestamp_ns": int(time.time_ns()),
                "funding_rate": float(rate),
                "next_funding_time_ns": int(next_ts) * 1_000_000 if next_ts else "",
            },
        )
