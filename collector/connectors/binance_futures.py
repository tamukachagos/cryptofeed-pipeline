"""Binance USDT-M Futures WebSocket connector.

Subscribes to:
  - <symbol>@depth@100ms    — incremental order book updates
  - <symbol>@aggTrade       — aggregated trades
  - <symbol>@markPrice@1s   — mark price + funding rate
  - <symbol>@kline_1m       — 1-minute OHLCV candles
  - <symbol>@forceOrder     — liquidation orders

Also polls REST for open interest every 60 seconds.

Publishes to Redis streams:
  binance:depth:<SYMBOL>
  binance:trades:<SYMBOL>
  binance:funding:<SYMBOL>
  binance:mark_price:<SYMBOL>
  binance:ohlcv:<SYMBOL>
  binance:liquidations:<SYMBOL>
  binance:open_interest:<SYMBOL>
"""
from __future__ import annotations

import asyncio
import json
import time

import redis.asyncio as aioredis
from loguru import logger

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

from collector.book_engine import OrderBookEngine
from collector.connectors.base import BaseConnector

_WS_BASE = "wss://fstream.binance.com/stream"
_REST_BASE = "https://fapi.binance.com"
_PING_INTERVAL = 20  # seconds
_OI_POLL_INTERVAL = 60  # seconds


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
            streams += [
                f"{s}@depth@100ms",
                f"{s}@aggTrade",
                f"{s}@markPrice@1s",
                f"{s}@kline_1m",
                f"{s}@forceOrder",
            ]
        return f"{_WS_BASE}?streams=" + "/".join(streams)

    async def run(self) -> None:
        """Override to also run OI polling alongside WebSocket."""
        self._running = True
        await asyncio.gather(
            self._ws_run_loop(),
            self._oi_poll_loop(),
        )

    async def _ws_run_loop(self) -> None:
        backoff = 1.0
        max_backoff = 60.0
        while self._running:
            try:
                await self._connect_and_stream()
                backoff = 1.0
            except Exception as exc:
                logger.warning(
                    f"[binance] Disconnected: {exc}. Reconnecting in {backoff:.0f}s…"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _oi_poll_loop(self) -> None:
        """Poll Binance REST for open interest every 60 seconds."""
        if not _AIOHTTP_AVAILABLE:
            logger.warning("[binance] aiohttp not available — OI polling disabled")
            return

        await asyncio.sleep(5)  # Let WS connect first
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    for sym in self.symbols:
                        try:
                            url = f"{_REST_BASE}/fapi/v1/openInterest"
                            async with session.get(url, params={"symbol": sym}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                                if r.status == 200:
                                    data = await r.json()
                                    oi = float(data.get("openInterest", 0))
                                    ts_ns = int(data.get("time", time.time() * 1000)) * 1_000_000
                                    await self._publish(
                                        f"binance:open_interest:{sym}",
                                        {
                                            "exchange": "binance",
                                            "symbol": sym,
                                            "timestamp_ns": ts_ns,
                                            "open_interest": oi,
                                            "open_interest_value": "",
                                        },
                                    )
                        except Exception as e:
                            logger.warning(f"[binance] OI poll failed for {sym}: {e}")
            except Exception as e:
                logger.warning(f"[binance] OI poll session error: {e}")
            await asyncio.sleep(_OI_POLL_INTERVAL)

    async def subscribe(self, ws) -> None:
        pass  # Combined stream URL handles subscription automatically

    async def _recv_loop(self, ws) -> None:
        last_ping = time.monotonic()
        async for message in ws:
            await self._handle_message(message)
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
            await self._handle_mark_price(symbol, data)
        elif event == "kline":
            await self._handle_kline(symbol, data)
        elif event == "forceOrder":
            await self._handle_liquidation(symbol, data)

    async def _handle_depth(self, symbol: str, data: dict) -> None:
        book = self._books.get(symbol)
        if book is None:
            return

        bids = data.get("b", [])
        asks = data.get("a", [])
        first_uid = int(data.get("U", 0))  # first update ID in event
        last_uid  = int(data.get("u", 0))  # last update ID in event

        if not book._initialized:
            # Buffer WS events per Binance spec §7.8:
            # 1. Buffer all events while REST snapshot fetch is in progress
            # 2. After snapshot arrives, discard buffered events with u < snap.lastUpdateId
            # 3. Apply buffered events where U <= snap.lastUpdateId+1 <= u, then continue
            if not hasattr(book, "_ws_buffer"):
                book._ws_buffer = []
                book._rest_fetching = False

            book._ws_buffer.append(data)

            if not book._rest_fetching:
                book._rest_fetching = True
                asyncio.create_task(self._init_book_from_rest_buffered(symbol, book))
            return

        # Validate sequential update: first_uid must equal last_seq + 1
        if book._last_seq != -1 and first_uid != book._last_seq + 1:
            # Sequence gap — re-initialize book from REST
            book._initialized = False
            book._gap_count   += 1
            if not hasattr(book, "_ws_buffer"):
                book._ws_buffer = []
            book._rest_fetching = False
            logger.warning(f"[binance] Seq gap on {symbol}: expected {book._last_seq + 1}, got {first_uid} — re-init")
            return

        book.apply_delta(bids, asks, seq=last_uid)

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

    async def _init_book_from_rest_buffered(self, symbol: str, book) -> None:
        """
        Fetch REST snapshot and replay buffered WS events per Binance spec §7.8.
        Called as a background task when the book is not yet initialized.
        """
        if not _AIOHTTP_AVAILABLE:
            # Fallback: initialize from oldest buffered event as a rough snapshot
            if hasattr(book, "_ws_buffer") and book._ws_buffer:
                oldest = book._ws_buffer[0]
                book.apply_snapshot(oldest.get("b", []), oldest.get("a", []),
                                    seq=int(oldest.get("u", -1)))
            book._ws_buffer = []
            book._rest_fetching = False
            return

        try:
            url = f"{_REST_BASE}/fapi/v1/depth"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"symbol": symbol, "limit": 1000},
                                       timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status != 200:
                        book._rest_fetching = False
                        return
                    snap = await r.json()

            snap_last_uid = int(snap.get("lastUpdateId", 0))
            book.apply_snapshot(snap.get("bids", []), snap.get("asks", []),
                                seq=snap_last_uid)
            logger.info(f"[binance] Book initialized from REST for {symbol} (lastUpdateId={snap_last_uid})")

            # Replay buffered events: discard those fully before the snapshot,
            # apply those that overlap or follow
            buffered = getattr(book, "_ws_buffer", [])
            for evt in buffered:
                u = int(evt.get("u", 0))   # last update ID
                U = int(evt.get("U", 0))   # first update ID
                if u <= snap_last_uid:
                    continue  # discard: event predates snapshot
                # Apply if it overlaps (U <= snap_last_uid + 1 <= u) or follows
                book.apply_delta(evt.get("b", []), evt.get("a", []), seq=u)

        except Exception as exc:
            logger.warning(f"[binance] Buffered book init failed for {symbol}: {exc}")
        finally:
            book._ws_buffer = []
            book._rest_fetching = False

    async def _init_book_from_rest(self, symbol: str, book, depth_event: dict) -> None:
        """
        Fetch REST snapshot and initialize book per Binance spec:
            1. Fetch GET /fapi/v1/depth?symbol=X&limit=1000
            2. Drop any buffered updates where u < snapshot.lastUpdateId
            3. Apply the snapshot, then replay buffered updates with U ≤ snap.lastUpdateId+1 ≤ u
        """
        if not _AIOHTTP_AVAILABLE:
            # Fallback: use WS event as snapshot (imprecise but usable)
            book.apply_snapshot(depth_event.get("b", []), depth_event.get("a", []),
                                seq=int(depth_event.get("u", -1)))
            return

        try:
            url = f"{_REST_BASE}/fapi/v1/depth"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"symbol": symbol, "limit": 1000},
                                       timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status != 200:
                        return
                    snap = await r.json()

            snap_last_uid = int(snap.get("lastUpdateId", 0))
            book.apply_snapshot(snap.get("bids", []), snap.get("asks", []),
                                seq=snap_last_uid)
            logger.info(f"[binance] Book initialized from REST snapshot for {symbol} "
                        f"(lastUpdateId={snap_last_uid})")
        except Exception as exc:
            logger.warning(f"[binance] REST book init failed for {symbol}: {exc}")

    async def _handle_trade(self, symbol: str, data: dict) -> None:
        side = "sell" if data.get("m") else "buy"
        await self._publish(
            f"binance:trades:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(data.get("T", 0)) * 1_000_000,
                "trade_id": str(data.get("l", "")),
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

    async def _handle_mark_price(self, symbol: str, data: dict) -> None:
        mp = data.get("p")
        ip = data.get("i")
        if mp is None:
            return
        await self._publish(
            f"binance:mark_price:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(data.get("E", 0)) * 1_000_000,
                "mark_price": float(mp),
                "index_price": float(ip) if ip else "",
            },
        )

    async def _handle_kline(self, symbol: str, data: dict) -> None:
        k = data.get("k", {})
        await self._publish(
            f"binance:ohlcv:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(k.get("t", 0)) * 1_000_000,
                "interval": str(k.get("i", "1m")),
                "open": float(k.get("o", 0)),
                "high": float(k.get("h", 0)),
                "low": float(k.get("l", 0)),
                "close": float(k.get("c", 0)),
                "volume": float(k.get("v", 0)),
                "quote_volume": float(k.get("q", 0)),
                "trades": int(k.get("n", 0)),
                "is_closed": int(bool(k.get("x", False))),
            },
        )

    async def _handle_liquidation(self, symbol: str, data: dict) -> None:
        order = data.get("o", {})
        # Binance forceOrder: side is the position side being liquidated
        # S="BUY" means a short position was liquidated (buy order placed)
        raw_side = order.get("S", "BUY")
        side = "buy" if raw_side == "BUY" else "sell"
        await self._publish(
            f"binance:liquidations:{symbol}",
            {
                "exchange": "binance",
                "symbol": symbol,
                "timestamp_ns": int(order.get("T", 0)) * 1_000_000,
                "side": side,
                "price": float(order.get("ap", order.get("p", 0))),  # avg price or limit price
                "qty": float(order.get("q", 0)),
                "order_type": str(order.get("o", "")).lower(),
            },
        )
