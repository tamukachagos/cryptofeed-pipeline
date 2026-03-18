"""Abstract base WebSocket connector with auto-reconnect and Redis publishing."""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod

import redis.asyncio as aioredis
from loguru import logger


class BaseConnector(ABC):
    """Base class for all exchange WebSocket connectors.

    Handles reconnect logic with exponential backoff. Subclasses implement
    `_ws_url`, `subscribe()`, and `_handle_message()`.
    """

    exchange: str = "base"

    def __init__(
        self,
        symbols: list[str],
        redis_client: aioredis.Redis,
        stream_maxlen: int = 100_000,
    ) -> None:
        self.symbols = symbols
        self.redis = redis_client
        self.stream_maxlen = stream_maxlen
        self._running = False

    @property
    @abstractmethod
    def _ws_url(self) -> str:
        """WebSocket URL for this exchange."""

    @abstractmethod
    async def subscribe(self, ws) -> None:
        """Send subscription messages after connection established."""

    @abstractmethod
    async def _handle_message(self, raw: str) -> None:
        """Parse and publish a raw WebSocket message."""

    async def run(self) -> None:
        """Main loop: connect, subscribe, receive messages with reconnect."""
        self._running = True
        backoff = 1.0
        max_backoff = 60.0

        while self._running:
            try:
                await self._connect_and_stream()
                backoff = 1.0  # Reset on clean disconnect
            except Exception as exc:
                logger.warning(
                    f"[{self.exchange}] Disconnected: {exc}. "
                    f"Reconnecting in {backoff:.0f}s…"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_and_stream(self) -> None:
        import websockets

        logger.info(f"[{self.exchange}] Connecting to {self._ws_url}")
        async with websockets.connect(
            self._ws_url,
            ping_interval=None,  # We handle ping manually
            close_timeout=5,
        ) as ws:
            logger.info(f"[{self.exchange}] Connected. Subscribing to {self.symbols}…")
            await self.subscribe(ws)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws) -> None:
        """Receive loop — subclasses can override for custom ping logic."""
        async for message in ws:
            await self._handle_message(message)

    async def _publish(self, stream: str, data: dict) -> None:
        """XADD to Redis stream, trimming to MAXLEN."""
        try:
            fields = {}
            for k, v in data.items():
                if isinstance(v, bool):
                    fields[k] = int(v)
                elif isinstance(v, (str, bytes, int, float)):
                    fields[k] = v
                else:
                    fields[k] = json.dumps(v)
            await self.redis.xadd(stream, fields, maxlen=self.stream_maxlen, approximate=True)
        except Exception as exc:
            logger.error(f"[{self.exchange}] Redis publish failed on {stream}: {exc}")

    def _now_ns(self) -> int:
        return int(time.time_ns())
