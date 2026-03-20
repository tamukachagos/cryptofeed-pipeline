"""Atomic partitioned Parquet writer.

Partitions by: data_type/exchange/symbol/YYYY-MM-DD/HH/data.parquet
Uses atomic write pattern: write to .tmp then os.rename() for durability.
Buffers rows in memory, flushing when row count or time threshold is reached.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


class ParquetWriter:
    """Buffers records and flushes them to partitioned Parquet files atomically."""

    def __init__(
        self,
        data_dir: str,
        data_type: str,
        compression: str = "zstd",
        flush_rows: int = 10_000,
        flush_secs: int = 60,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_type = data_type  # "trades", "books", "funding"
        self.compression = compression
        self.flush_rows = flush_rows
        self.flush_secs = flush_secs
        # key = "exchange/symbol/YYYY-MM-DD/HH" → list of record dicts
        self._buffers: dict[str, list[dict]] = {}
        self._last_flush: dict[str, float] = {}

    def write(self, record: dict, exchange: str, symbol: str, timestamp_ns: int) -> None:
        """Buffer one record; flush if thresholds exceeded."""
        dt = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
        hour_key = dt.strftime("%Y-%m-%d/%H")
        key = f"{exchange}/{symbol}/{hour_key}"

        if key not in self._buffers:
            self._buffers[key] = []
            self._last_flush[key] = time.monotonic()

        self._buffers[key].append(record)

        elapsed = time.monotonic() - self._last_flush[key]
        if len(self._buffers[key]) >= self.flush_rows or elapsed >= self.flush_secs:
            self._flush(key, exchange, symbol, hour_key)

    def _flush(self, key: str, exchange: str, symbol: str, hour_key: str) -> None:
        """Write buffered rows to Parquet atomically.

        On OSError (e.g. disk full), rows are re-queued into the buffer so they
        are not lost. The next write() call will retry after the threshold is
        reached again.
        """
        rows = self._buffers.pop(key, [])
        self._last_flush.pop(key, None)
        if not rows:
            return

        out_dir = self.data_dir / self.data_type / exchange / symbol / hour_key
        tmp_path = out_dir / "data.parquet.tmp"
        out_path = out_dir / "data.parquet"

        try:
            out_dir.mkdir(parents=True, exist_ok=True)

            df = pd.DataFrame(rows)

            # Merge with existing file if it exists (append mode)
            if out_path.exists():
                existing = pd.read_parquet(out_path)
                df = pd.concat([existing, df], ignore_index=True)
                if "timestamp_ns" in df.columns:
                    df = df.drop_duplicates("timestamp_ns").sort_values("timestamp_ns")

            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, tmp_path, compression=self.compression)
            os.replace(tmp_path, out_path)  # Atomic rename

            logger.debug(
                f"[ParquetWriter] Flushed {len(rows)} rows → "
                f"{self.data_type}/{exchange}/{symbol}/{hour_key}"
            )
        except OSError as exc:
            # Disk full or other I/O error — re-queue rows to avoid data loss
            logger.error(
                f"[ParquetWriter] Flush failed for {key}: {exc}. "
                f"Re-queuing {len(rows)} rows for next flush attempt."
            )
            if key not in self._buffers:
                self._buffers[key] = []
                self._last_flush[key] = time.monotonic()
            self._buffers[key] = rows + self._buffers[key]
            # Clean up partial .tmp file if it was created
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

    def flush_all(self) -> None:
        """Force flush all buffers. Call on shutdown."""
        for key in list(self._buffers.keys()):
            parts = key.split("/")
            exchange, symbol = parts[0], parts[1]
            hour_key = "/".join(parts[2:])
            self._flush(key, exchange, symbol, hour_key)
        logger.info("[ParquetWriter] All buffers flushed.")
