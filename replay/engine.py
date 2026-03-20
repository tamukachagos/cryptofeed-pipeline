"""Deterministic tick-by-tick replay engine.

Reads Parquet files partitioned by exchange/symbol/date/hour and yields
normalized events in chronological order. Suitable for event-driven backtesting.

Usage:
    from datetime import datetime, timezone
    from replay.engine import ReplayEngine

    engine = ReplayEngine(data_dir="./data")
    start = datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    end   = datetime(2025, 3, 17, 1, 0, tzinfo=timezone.utc)

    for event in engine.iter_merged("binance", "BTCUSDT", start, end):
        print(event)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Union

import pandas as pd
import pyarrow.parquet as pq

from processor.schemas import NormalizedBookSnapshot, NormalizedFunding, NormalizedTrade


class ReplayEngine:
    """Loads Parquet files and yields events in timestamp order."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)

    # ------------------------------------------------------------------
    # Public iterators
    # ------------------------------------------------------------------

    def iter_trades(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> Iterator[NormalizedTrade]:
        """Yield NormalizedTrade events in chronological order."""
        df = self._load("trades", exchange, symbol, start, end)
        for _, row in df.iterrows():
            yield NormalizedTrade(
                exchange=row["exchange"],
                symbol=row["symbol"],
                timestamp_ns=int(row["timestamp_ns"]),
                trade_id=str(row.get("trade_id", "")),
                price=float(row["price"]),
                qty=float(row["qty"]),
                side=str(row["side"]),
                is_liquidation=bool(row.get("is_liquidation", False)),
            )

    def iter_books(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> Iterator[NormalizedBookSnapshot]:
        """Yield NormalizedBookSnapshot events in chronological order."""
        df = self._load("books", exchange, symbol, start, end)
        for _, row in df.iterrows():
            bids = row.get("bids", "[]")
            asks = row.get("asks", "[]")
            if isinstance(bids, str):
                bids = json.loads(bids)
            if isinstance(asks, str):
                asks = json.loads(asks)
            yield NormalizedBookSnapshot(
                exchange=row["exchange"],
                symbol=row["symbol"],
                timestamp_ns=int(row["timestamp_ns"]),
                seq=int(row.get("seq", -1)),
                bids=bids,
                asks=asks,
                spread=_opt_float(row.get("spread")),
                midprice=_opt_float(row.get("midprice")),
                microprice=_opt_float(row.get("microprice")),
                obi_5=_opt_float(row.get("obi_5")),
            )

    def iter_funding(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> Iterator[NormalizedFunding]:
        """Yield NormalizedFunding events in chronological order."""
        df = self._load("funding", exchange, symbol, start, end)
        for _, row in df.iterrows():
            nft = row.get("next_funding_time_ns")
            yield NormalizedFunding(
                exchange=row["exchange"],
                symbol=row["symbol"],
                timestamp_ns=int(row["timestamp_ns"]),
                funding_rate=float(row["funding_rate"]),
                next_funding_time_ns=int(nft) if nft and not pd.isna(nft) else None,
            )

    def iter_merged(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> Iterator[Union[NormalizedTrade, NormalizedBookSnapshot, NormalizedFunding]]:
        """Yield all event types merged by timestamp_ns (chronological order)."""
        import heapq

        # Wrap each iterator with (timestamp_ns, counter, event) for heapq ordering
        counter = [0]

        def tagged(it):
            for event in it:
                counter[0] += 1
                yield (event.timestamp_ns, counter[0], event)

        iterators = [
            tagged(self.iter_trades(exchange, symbol, start, end)),
            tagged(self.iter_books(exchange, symbol, start, end)),
            tagged(self.iter_funding(exchange, symbol, start, end)),
        ]

        for _, _, event in heapq.merge(*iterators):
            yield event

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(
        self, data_type: str, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        """Load and concatenate all Parquet files in the time range."""
        start_ns = int(start.replace(tzinfo=timezone.utc).timestamp() * 1e9) if start.tzinfo is None else int(start.timestamp() * 1e9)
        end_ns = int(end.replace(tzinfo=timezone.utc).timestamp() * 1e9) if end.tzinfo is None else int(end.timestamp() * 1e9)

        base = self.data_dir / data_type / exchange / symbol
        if not base.exists():
            return pd.DataFrame()

        # PyArrow push-down filter: skip decoding row groups outside the time range
        pq_filters = [
            ("timestamp_ns", ">=", start_ns),
            ("timestamp_ns", "<",  end_ns),
        ]

        dfs = []
        current = start.replace(minute=0, second=0, microsecond=0)
        from datetime import timedelta
        while current <= end:
            hour_path = base / current.strftime("%Y-%m-%d") / str(current.hour) / "data.parquet"
            if hour_path.exists():
                try:
                    table = pq.read_table(hour_path, filters=pq_filters)
                    if table.num_rows:
                        dfs.append(table.to_pandas())
                except Exception:
                    pass
            current = current + timedelta(hours=1)

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        if "timestamp_ns" in df.columns:
            df = df.sort_values("timestamp_ns").reset_index(drop=True)
        return df


def _opt_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
