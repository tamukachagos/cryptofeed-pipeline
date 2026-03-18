"""Data quality validation for collected market data."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from loguru import logger


class DataQualityChecker:
    """Validates data quality for a given exchange/symbol/date."""

    def check_gaps(self, df: pd.DataFrame, expected_interval_ms: int) -> dict:
        """Find timestamp gaps larger than 2x the expected interval.

        Args:
            df: DataFrame with timestamp_ns column.
            expected_interval_ms: Expected time between records in milliseconds.

        Returns:
            dict with keys: n_gaps, max_gap_ms, gap_locations (list of timestamps).
        """
        if df.empty or "timestamp_ns" not in df.columns:
            return {"n_gaps": 0, "max_gap_ms": 0, "gap_locations": []}

        ts = df["timestamp_ns"].sort_values().reset_index(drop=True)
        diffs_ms = ts.diff().dropna() / 1_000_000  # ns → ms
        threshold_ms = expected_interval_ms * 2

        gaps = diffs_ms[diffs_ms > threshold_ms]
        gap_locations = ts.iloc[gaps.index].tolist()

        return {
            "n_gaps": len(gaps),
            "max_gap_ms": float(gaps.max()) if not gaps.empty else 0.0,
            "gap_locations": gap_locations[:20],  # First 20 gap timestamps
        }

    def check_crossed_book(self, book_df: pd.DataFrame) -> int:
        """Count rows where best_bid >= best_ask (invalid book state).

        Works with either numeric spread column or bids/asks JSON columns.
        """
        if book_df.empty:
            return 0

        if "spread" in book_df.columns:
            spread = book_df["spread"].dropna().astype(float)
            return int((spread <= 0).sum())

        crossed = 0
        for _, row in book_df.iterrows():
            try:
                bids = json.loads(row["bids"]) if isinstance(row["bids"], str) else row["bids"]
                asks = json.loads(row["asks"]) if isinstance(row["asks"], str) else row["asks"]
                if bids and asks:
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    if best_bid >= best_ask:
                        crossed += 1
            except Exception:
                pass
        return crossed

    def check_sequence_gaps(self, df: pd.DataFrame) -> int:
        """Count non-consecutive sequence numbers in a book DataFrame."""
        if df.empty or "seq" not in df.columns:
            return 0

        seqs = df["seq"].dropna().astype(int).sort_values().reset_index(drop=True)
        diffs = seqs.diff().dropna()
        return int((diffs != 1).sum())

    def check_price_outliers(self, df: pd.DataFrame, price_col: str = "price", z_thresh: float = 10.0) -> int:
        """Count price values more than z_thresh standard deviations from the mean."""
        if df.empty or price_col not in df.columns:
            return 0
        prices = df[price_col].dropna().astype(float)
        if len(prices) < 2:
            return 0
        z = (prices - prices.mean()) / prices.std()
        return int((z.abs() > z_thresh).sum())

    def generate_report(
        self, exchange: str, symbol: str, date: str, data_dir: str
    ) -> dict:
        """Generate a full quality report for one day of data.

        Args:
            exchange: e.g. "binance"
            symbol: e.g. "BTCUSDT"
            date: e.g. "2025-03-17"
            data_dir: Root data directory.

        Returns:
            dict with quality metrics per data type.
        """
        base = Path(data_dir)
        report = {
            "exchange": exchange,
            "symbol": symbol,
            "date": date,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        for data_type in ("trades", "books", "funding"):
            day_dir = base / data_type / exchange / symbol / date
            if not day_dir.exists():
                report[data_type] = {"status": "no_data"}
                continue

            dfs = []
            for hour_dir in sorted(day_dir.iterdir()):
                pq_file = hour_dir / "data.parquet"
                if pq_file.exists():
                    try:
                        dfs.append(pd.read_parquet(pq_file))
                    except Exception as exc:
                        logger.warning(f"Could not read {pq_file}: {exc}")

            if not dfs:
                report[data_type] = {"status": "empty"}
                continue

            df = pd.concat(dfs, ignore_index=True)
            n_rows = len(df)

            result: dict = {"status": "ok", "n_rows": n_rows}

            if data_type == "trades":
                result["gap_check"] = self.check_gaps(df, expected_interval_ms=1000)
                result["price_outliers"] = self.check_price_outliers(df)
                if "side" in df.columns:
                    result["buy_ratio"] = float((df["side"] == "buy").mean())

            elif data_type == "books":
                result["crossed_books"] = self.check_crossed_book(df)
                result["sequence_gaps"] = self.check_sequence_gaps(df)
                result["gap_check"] = self.check_gaps(df, expected_interval_ms=100)

            elif data_type == "funding":
                result["gap_check"] = self.check_gaps(df, expected_interval_ms=8 * 3600 * 1000)

            report[data_type] = result

        return report
