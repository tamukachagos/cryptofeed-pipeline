"""Demo: generate synthetic BTCUSDT trades, save as Parquet, replay with features.

Run:
    python scripts/replay_demo.py

If no real data exists, generates 1 hour of synthetic data (random walk prices,
buy/sell sides, realistic timestamps) and demonstrates the full pipeline.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from loguru import logger

from features.microstructure import compute_kyle_lambda, compute_trade_imbalance, compute_vpin
from processor.parquet_writer import ParquetWriter
from replay.engine import ReplayEngine


def generate_synthetic_trades(
    symbol: str = "BTCUSDT",
    exchange: str = "binance",
    start: datetime | None = None,
    n_trades: int = 3600,
) -> pd.DataFrame:
    """Generate synthetic trade data with a random walk price process."""
    if start is None:
        start = datetime(2025, 3, 17, 13, 0, 0, tzinfo=timezone.utc)

    rng = np.random.default_rng(42)
    price = 65_000.0
    prices = []
    timestamps_ns = []
    sides = []
    qtys = []

    for i in range(n_trades):
        # Random walk
        price += rng.normal(0, 15)
        price = max(price, 1000.0)

        # Random interval: ~1 trade per second with jitter
        ts_ns = int(start.timestamp() * 1e9) + i * 1_000_000_000 + int(rng.uniform(-200_000_000, 200_000_000))
        side = "buy" if rng.random() > 0.5 else "sell"
        qty = float(rng.exponential(0.05))

        prices.append(price)
        timestamps_ns.append(ts_ns)
        sides.append(side)
        qtys.append(round(qty, 6))

    return pd.DataFrame({
        "exchange": exchange,
        "symbol": symbol,
        "timestamp_ns": timestamps_ns,
        "trade_id": [str(i) for i in range(n_trades)],
        "price": prices,
        "qty": qtys,
        "side": sides,
        "is_liquidation": False,
    })


def main() -> None:
    data_dir = os.getenv("DATA_DIR", "./data")
    exchange = "binance"
    symbol = "BTCUSDT"
    start = datetime(2025, 3, 17, 13, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 3, 17, 14, 0, 0, tzinfo=timezone.utc)

    # Check if real data exists
    real_path = Path(data_dir) / "trades" / exchange / symbol / "2025-03-17" / "13" / "data.parquet"
    if real_path.exists():
        logger.info(f"Real data found at {real_path} — using it for replay.")
    else:
        logger.info("No real data found. Generating synthetic BTCUSDT trades…")
        trades_df = generate_synthetic_trades(symbol=symbol, exchange=exchange, start=start, n_trades=3600)
        logger.info(f"Generated {len(trades_df)} synthetic trades.")

        writer = ParquetWriter(data_dir, "trades", compression="snappy", flush_rows=5000, flush_secs=9999)
        for _, row in trades_df.iterrows():
            writer.write(row.to_dict(), exchange, symbol, int(row["timestamp_ns"]))
        writer.flush_all()
        logger.info(f"Saved synthetic data to {data_dir}/trades/{exchange}/{symbol}/")

    # Replay
    engine = ReplayEngine(data_dir=data_dir)
    logger.info(f"Replaying {exchange}/{symbol} from {start} to {end}…")

    trades = list(engine.iter_trades(exchange, symbol, start, end))
    if not trades:
        logger.error("No trades found after replay. Check DATA_DIR.")
        return

    logger.info(f"Replayed {len(trades)} trade events.")

    # Build DataFrame for feature computation
    trade_df = pd.DataFrame([t.model_dump() for t in trades])

    # Compute features
    imbalance = compute_trade_imbalance(trade_df, window=100)
    vpin = compute_vpin(trade_df, bucket_size=0.1)
    kyle_lam = compute_kyle_lambda(trade_df)

    print("\n=== Replay Summary ===")
    print(f"  Exchange : {exchange}")
    print(f"  Symbol   : {symbol}")
    print(f"  Period   : {start} to {end}")
    print(f"  Trades   : {len(trade_df)}")
    print(f"  Price range: {trade_df['price'].min():.2f} – {trade_df['price'].max():.2f}")
    print(f"  Buy ratio: {(trade_df['side'] == 'buy').mean():.1%}")

    print("\n=== Microstructure Features ===")
    print(f"  Trade Imbalance (last 100): {imbalance.iloc[-1]:.3f}"
          f"  [min={imbalance.min():.3f}, max={imbalance.max():.3f}]")
    print(f"  VPIN (10% buckets): {vpin.mean():.3f} avg  [{len(vpin)} buckets]")
    print(f"  Kyle's Lambda: {kyle_lam:.6f}")

    # Show a sample of events with features
    sample = trade_df[["timestamp_ns", "price", "qty", "side"]].head(10).copy()
    sample["timestamp"] = pd.to_datetime(sample["timestamp_ns"], unit="ns", utc=True)
    sample["imbalance"] = imbalance.head(10).values
    print("\n=== First 10 Trades ===")
    print(sample[["timestamp", "price", "qty", "side", "imbalance"]].to_string(index=False))
    print()


if __name__ == "__main__":
    main()
