"""Processor entry point.

Reads from Redis Streams (trades, books, funding), normalizes each message,
and writes to partitioned Parquet files via ParquetWriter.

Uses Redis consumer groups for reliable at-least-once processing.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from loguru import logger

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import redis.asyncio as aioredis

from processor.normalizer import BOOK_NORMALIZERS, FUNDING_NORMALIZERS, TRADE_NORMALIZERS
from processor.parquet_writer import ParquetWriter

GROUP_NAME = "processor"
CONSUMER_NAME = "processor-1"
BLOCK_MS = 1000
BATCH_SIZE = 100


def _build_stream_names(exchanges: list[str], symbols: list[str]) -> dict[str, str]:
    """Map stream name → data_type."""
    streams = {}
    for ex in exchanges:
        for sym in symbols:
            streams[f"{ex}:trades:{sym}"] = "trades"
            streams[f"{ex}:depth:{sym}"] = "books"
            if ex in ("binance", "okx"):
                streams[f"{ex}:funding:{sym}"] = "funding"
    return streams


async def ensure_groups(redis: aioredis.Redis, stream_names: list[str]) -> None:
    for stream in stream_names:
        try:
            await redis.xgroup_create(stream, GROUP_NAME, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Could not create group for {stream}: {e}")


async def process_streams(
    redis: aioredis.Redis,
    stream_type_map: dict[str, str],
    writers: dict[str, ParquetWriter],
) -> None:
    streams = {s: ">" for s in stream_type_map}
    stream_list = list(stream_type_map.keys())

    logger.info(f"[Processor] Listening on {len(stream_list)} streams…")

    while True:
        try:
            results = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams,
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )
        except aioredis.ResponseError as exc:
            logger.error(f"[Processor] xreadgroup error: {exc}")
            await asyncio.sleep(1)
            continue

        if not results:
            continue

        for stream_name, messages in results:
            data_type = stream_type_map.get(stream_name, "")
            exchange = stream_name.split(":")[0]

            for msg_id, fields in messages:
                try:
                    await _process_message(fields, data_type, exchange, writers)
                    await redis.xack(stream_name, GROUP_NAME, msg_id)
                except Exception as exc:
                    logger.error(f"[Processor] Failed on {stream_name}/{msg_id}: {exc}")


async def _process_message(
    fields: dict,
    data_type: str,
    exchange: str,
    writers: dict[str, ParquetWriter],
) -> None:
    writer = writers.get(data_type)
    if writer is None:
        return

    symbol = str(fields.get("symbol", ""))
    timestamp_ns = int(fields.get("timestamp_ns", 0))

    if data_type == "trades":
        fn = TRADE_NORMALIZERS.get(exchange)
        if fn:
            norm = fn(fields)
            writer.write(norm.model_dump(), exchange, symbol, timestamp_ns)

    elif data_type == "books":
        fn = BOOK_NORMALIZERS.get(exchange)
        if fn:
            norm = fn(fields)
            record = norm.model_dump()
            # Serialize nested lists to JSON strings for Parquet compatibility
            record["bids"] = json.dumps(record["bids"])
            record["asks"] = json.dumps(record["asks"])
            writer.write(record, exchange, symbol, timestamp_ns)

    elif data_type == "funding":
        fn = FUNDING_NORMALIZERS.get(exchange)
        if fn:
            norm = fn(fields)
            writer.write(norm.model_dump(), exchange, symbol, timestamp_ns)


async def main() -> None:
    symbols_raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]

    enabled = []
    if os.getenv("ENABLE_BINANCE", "true").lower() == "true":
        enabled.append("binance")
    if os.getenv("ENABLE_BYBIT", "true").lower() == "true":
        enabled.append("bybit")
    if os.getenv("ENABLE_OKX", "true").lower() == "true":
        enabled.append("okx")

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis = await aioredis.from_url(
        f"redis://{redis_host}:{redis_port}",
        decode_responses=True,
    )

    data_dir = os.getenv("DATA_DIR", "./data")
    compression = os.getenv("PARQUET_COMPRESSION", "zstd")
    flush_rows = int(os.getenv("PARQUET_FLUSH_ROWS", 10_000))
    flush_secs = int(os.getenv("PARQUET_FLUSH_SECS", 60))

    writers = {
        dt: ParquetWriter(data_dir, dt, compression, flush_rows, flush_secs)
        for dt in ("trades", "books", "funding")
    }

    stream_type_map = _build_stream_names(enabled, symbols)
    await ensure_groups(redis, list(stream_type_map.keys()))

    loop = asyncio.get_event_loop()

    def _shutdown():
        logger.info("[Processor] Shutting down, flushing all buffers…")
        for w in writers.values():
            w.flush_all()
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass  # Windows

    await process_streams(redis, stream_type_map, writers)


if __name__ == "__main__":
    asyncio.run(main())
