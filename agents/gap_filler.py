"""Gap Filler Agent — detects and backfills trade data gaps using Binance REST API.

Usage:
    python agents/gap_filler.py
    python agents/gap_filler.py --days 1 --symbol BTCUSDT
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import pandas as pd
import requests
from loguru import logger

GAP_LOG = Path("logs/gap_fills.jsonl")
MIN_GAP_S = 300         # Only fill gaps > 5 minutes
BINANCE_FAPI = "https://fapi.binance.com"
EXCHANGES_WITH_REST = ["binance"]


def find_gaps(exchange: str, symbol: str, date_str: str) -> list[dict]:
    """Find timestamp gaps > MIN_GAP_S in a day's trade data."""
    from replay.engine import ReplayEngine
    data_dir = os.getenv("DATA_DIR", "./data")

    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    engine = ReplayEngine(data_dir=data_dir)
    df = engine._load("trades", exchange, symbol, start, end)

    if df.empty or "timestamp_ns" not in df.columns:
        return []

    ts = df["timestamp_ns"].sort_values().reset_index(drop=True)
    diffs_s = ts.diff().dropna() / 1e9

    gaps = []
    for i, gap_s in diffs_s.items():
        if gap_s > MIN_GAP_S:
            gap_start_ns = int(ts.iloc[i - 1])
            gap_end_ns = int(ts.iloc[i])
            gaps.append({
                "exchange": exchange,
                "symbol": symbol,
                "date": date_str,
                "gap_start_ns": gap_start_ns,
                "gap_end_ns": gap_end_ns,
                "gap_s": gap_s,
            })
    return gaps


def fetch_binance_trades_rest(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch historical aggTrades from Binance REST API to fill a gap."""
    url = f"{BINANCE_FAPI}/fapi/v1/aggTrades"
    all_rows = []
    cursor_ms = start_ms

    while cursor_ms < end_ms:
        params = {
            "symbol": symbol,
            "startTime": cursor_ms,
            "endTime": min(cursor_ms + 3_600_000, end_ms),  # 1h chunks
            "limit": 1000,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            logger.error(f"Binance REST fetch failed: {exc}")
            break

        if not batch:
            break

        all_rows.extend(batch)
        last_ts = batch[-1]["T"]
        if last_ts >= end_ms - 1 or len(batch) < 1000:
            break
        cursor_ms = last_ts + 1
        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["exchange"] = "binance"
    df["symbol"] = symbol
    df["timestamp_ns"] = (df["T"].astype("int64") * 1_000_000)
    df["trade_id"] = df["l"].astype(str)  # last trade ID in aggregate
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["side"] = df["m"].apply(lambda m: "sell" if m else "buy")
    df["is_liquidation"] = False

    return df[["exchange", "symbol", "timestamp_ns", "trade_id", "price", "qty", "side", "is_liquidation"]]


def fill_gap(gap: dict) -> bool:
    """Fetch and write data for a single gap. Returns True if filled."""
    from processor.parquet_writer import ParquetWriter

    exchange = gap["exchange"]
    symbol = gap["symbol"]
    start_ms = gap["gap_start_ns"] // 1_000_000
    end_ms = gap["gap_end_ns"] // 1_000_000

    logger.info(f"Filling gap: {exchange}/{symbol} {gap['gap_s']:.0f}s gap")

    if exchange != "binance":
        logger.warning(f"REST backfill only supported for Binance (got {exchange})")
        return False

    df = fetch_binance_trades_rest(symbol, start_ms, end_ms)
    if df.empty:
        logger.warning(f"No data returned for gap fill")
        return False

    data_dir = os.getenv("DATA_DIR", "./data")
    writer = ParquetWriter(data_dir, "trades", compression="zstd", flush_rows=99999, flush_secs=99999)
    for _, row in df.iterrows():
        writer.write(row.to_dict(), exchange, symbol, int(row["timestamp_ns"]))
    writer.flush_all()

    logger.success(f"Filled {len(df)} trades for {exchange}/{symbol}")

    # Log fill
    GAP_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(GAP_LOG, "a") as f:
        f.write(json.dumps({
            **gap,
            "filled_trades": len(df),
            "filled_at": datetime.now(timezone.utc).isoformat(),
        }) + "\n")

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1, help="How many past days to scan")
    parser.add_argument("--symbol", default="BTCUSDT")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    symbols = args.symbol.split(",")

    total_gaps = 0
    total_filled = 0

    for d in range(args.days):
        date = today - timedelta(days=d)
        date_str = date.strftime("%Y-%m-%d")

        for symbol in symbols:
            for exchange in EXCHANGES_WITH_REST:
                gaps = find_gaps(exchange, symbol, date_str)
                total_gaps += len(gaps)
                if gaps:
                    logger.info(f"Found {len(gaps)} gap(s) in {exchange}/{symbol}/{date_str}")
                    for gap in gaps:
                        if fill_gap(gap):
                            total_filled += 1
                else:
                    logger.info(f"No gaps: {exchange}/{symbol}/{date_str}")

    print(f"\nGap fill complete: {total_filled}/{total_gaps} gaps filled.")


if __name__ == "__main__":
    main()
