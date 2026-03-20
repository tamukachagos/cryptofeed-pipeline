"""Collector entry point: starts all WebSocket connectors concurrently."""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from loguru import logger

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import redis.asyncio as aioredis

from collector.connectors.binance_futures import BinanceFuturesConnector
from collector.connectors.bybit import BybitConnector
from collector.connectors.okx import OKXConnector


async def main() -> None:
    symbols_raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]

    redis_host     = os.getenv("REDIS_HOST", "localhost")
    redis_port     = int(os.getenv("REDIS_PORT", 6379))
    redis_password = os.getenv("REDIS_PASSWORD", "")
    stream_maxlen  = int(os.getenv("REDIS_STREAM_MAXLEN", 100_000))

    redis_url = (
        f"redis://:{redis_password}@{redis_host}:{redis_port}"
        if redis_password else
        f"redis://{redis_host}:{redis_port}"
    )
    redis_client = await aioredis.from_url(redis_url, decode_responses=True)

    connectors = []

    if os.getenv("ENABLE_BINANCE", "true").lower() == "true":
        connectors.append(
            BinanceFuturesConnector(symbols=symbols, redis_client=redis_client, stream_maxlen=stream_maxlen)
        )
        logger.info(f"Binance Futures connector enabled for {symbols}")

    if os.getenv("ENABLE_BYBIT", "true").lower() == "true":
        connectors.append(
            BybitConnector(symbols=symbols, redis_client=redis_client, stream_maxlen=stream_maxlen)
        )
        logger.info(f"Bybit connector enabled for {symbols}")

    if os.getenv("ENABLE_OKX", "true").lower() == "true":
        connectors.append(
            OKXConnector(symbols=symbols, redis_client=redis_client, stream_maxlen=stream_maxlen)
        )
        logger.info(f"OKX connector enabled for {symbols}")

    if not connectors:
        logger.error("No connectors enabled. Set ENABLE_BINANCE/ENABLE_BYBIT/ENABLE_OKX in .env")
        return

    logger.info(f"Starting {len(connectors)} connector(s)…")
    await asyncio.gather(*[c.run() for c in connectors])


if __name__ == "__main__":
    asyncio.run(main())
