"""OKX Futures WebSocket connector (V5 API).

Subscribes to:
  - books            — order book (L2, 400 levels)
  - trades           — public trade feed
  - funding-rate     — funding rate updates
  - candle1m         — 1-minute OHLCV candles
  - mark-price       — mark price
  - open-interest    — open interest
  - liquidation-orders (instType=SWAP) — liquidation events

Symbol conversion: BTCUSDT → BTC-USDT-SWAP

Publishes to Redis streams:
  okx:depth:<SYMBOL>
  okx:trades:<SYMBOL>
  okx:funding:<SYMBOL>
  okx:ohlcv:<SYMBOL>
  okx:mark_price:<SYMBOL>
  okx:open_interest:<SYMBOL>
  okx:liquidations:<SYMBOL>
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
                {"channel": "candle1m", "instId": inst},
                {"channel": "mark-price", "instId": inst},
                {"channel": "open-interest", "instId": inst},
            ]
        # Liquidation-orders is per instType, not per instId
        args.append({"channel": "liquidation-orders", "instType": "SWAP"})

        sub_msg = json.dumps({"op": "subscribe", "args": args})
        await ws.send(sub_msg)
        logger.info(f"[okx] Subscribed for {self.symbols}")

    async def _recv_loop(self, ws) -> None:
        last_ping = time.monotonic()
        async for message in ws:
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

        if "event" in msg:
            return

        channel = msg.get("arg", {}).get("channel", "")
        inst_id = msg.get("arg", {}).get("instId", "")
        symbol = self._inst_to_sym.get(inst_id)

        action = msg.get("action", "snapshot")
        data_list = msg.get("data", [])
        if not data_list:
            return

        if channel == "books":
            if symbol is None:
                return
            await self._handle_depth(symbol, data_list[0], action)
        elif channel == "trades":
            if symbol is None:
                return
            await self._handle_trades(symbol, data_list)
        elif channel == "funding-rate":
            if symbol is None:
                return
            await self._handle_funding(symbol, data_list[0])
        elif channel == "candle1m":
            if symbol is None:
                return
            await self._handle_ohlcv(symbol, data_list)
        elif channel == "mark-price":
            if symbol is None:
                return
            await self._handle_mark_price(symbol, data_list[0])
        elif channel == "open-interest":
            if symbol is None:
                return
            await self._handle_oi(symbol, data_list[0])
        elif channel == "liquidation-orders":
            await self._handle_liquidations(data_list)

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

    async def _handle_ohlcv(self, symbol: str, candles: list) -> None:
        # OKX candle format: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        for c in candles:
            if len(c) < 6:
                continue
            is_closed = len(c) >= 9 and c[8] == "1"
            await self._publish(
                f"okx:ohlcv:{symbol}",
                {
                    "exchange": "okx",
                    "symbol": symbol,
                    "timestamp_ns": int(c[0]) * 1_000_000,
                    "interval": "1m",
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                    "quote_volume": float(c[7]) if len(c) >= 8 else 0.0,
                    "is_closed": int(is_closed),
                },
            )

    async def _handle_mark_price(self, symbol: str, data: dict) -> None:
        mp = data.get("markPx")
        if mp is None:
            return
        await self._publish(
            f"okx:mark_price:{symbol}",
            {
                "exchange": "okx",
                "symbol": symbol,
                "timestamp_ns": int(data.get("ts", 0)) * 1_000_000,
                "mark_price": float(mp),
                "index_price": "",
            },
        )

    async def _handle_oi(self, symbol: str, data: dict) -> None:
        oi = data.get("oi")
        oiv = data.get("oiCcy")
        if oi is None:
            return
        await self._publish(
            f"okx:open_interest:{symbol}",
            {
                "exchange": "okx",
                "symbol": symbol,
                "timestamp_ns": int(data.get("ts", 0)) * 1_000_000,
                "open_interest": float(oi),
                "open_interest_value": float(oiv) if oiv else "",
            },
        )

    async def _handle_liquidations(self, data_list: list) -> None:
        for item in data_list:
            details = item.get("details", [])
            inst_id = item.get("instId", "")
            symbol = self._inst_to_sym.get(inst_id)
            if symbol is None:
                # Try to derive from instId (may have instruments not in our list)
                symbol = _from_inst_id(inst_id)
                # Only process symbols we care about
                if symbol not in {s for s in self._inst_to_sym.values()}:
                    continue
            for d in details:
                side = d.get("side", "buy").lower()
                await self._publish(
                    f"okx:liquidations:{symbol}",
                    {
                        "exchange": "okx",
                        "symbol": symbol,
                        "timestamp_ns": int(d.get("ts", 0)) * 1_000_000,
                        "side": side,
                        "price": float(d.get("bkPx", 0)),
                        "qty": float(d.get("sz", 0)),
                        "order_type": "market",
                    },
                )
