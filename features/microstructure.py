"""Microstructure feature computations.

All functions accept DataFrames produced by the ReplayEngine and return
pd.Series (or scalars) aligned to the input index.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_obi(book_df: pd.DataFrame, levels: int = 5) -> pd.Series:
    """Order book imbalance from pre-computed obi_5 column.

    If obi_{levels} column exists, returns it directly.
    Otherwise recomputes from bids/asks JSON columns.

    Returns: Series of floats in [-1, 1].
    """
    col = f"obi_{levels}"
    if col in book_df.columns:
        return book_df[col].astype(float)

    import json

    def _obi_row(row):
        try:
            bids = json.loads(row["bids"]) if isinstance(row["bids"], str) else row["bids"]
            asks = json.loads(row["asks"]) if isinstance(row["asks"], str) else row["asks"]
            bid_vol = sum(float(b[1]) for b in bids[:levels])
            ask_vol = sum(float(a[1]) for a in asks[:levels])
            total = bid_vol + ask_vol
            return (bid_vol - ask_vol) / total if total > 0 else np.nan
        except Exception:
            return np.nan

    return book_df.apply(_obi_row, axis=1)


def compute_trade_imbalance(trade_df: pd.DataFrame, window: int = 100) -> pd.Series:
    """Rolling buy/sell volume imbalance.

    Returns: Series of floats in [0, 1] where:
        1.0 = all buys in window
        0.0 = all sells in window
        0.5 = balanced
    """
    if trade_df.empty:
        return pd.Series(dtype=float)

    df = trade_df.copy()
    df["is_buy"] = (df["side"] == "buy").astype(float)
    df["buy_vol"] = df["qty"] * df["is_buy"]
    df["sell_vol"] = df["qty"] * (1 - df["is_buy"])

    rolling_buy = df["buy_vol"].rolling(window, min_periods=1).sum()
    rolling_sell = df["sell_vol"].rolling(window, min_periods=1).sum()
    total = rolling_buy + rolling_sell

    return (rolling_buy / total).where(total > 0, other=np.nan)


def compute_realized_spread(
    trade_df: pd.DataFrame,
    midprice_series: pd.Series,
    horizon_steps: int = 5,
) -> pd.Series:
    """Realized spread: sign(trade) * (trade_price - future_midprice).

    Args:
        trade_df: DataFrame with columns [timestamp_ns, price, side].
        midprice_series: Series of midprices aligned to trade timestamps.
        horizon_steps: How many trades forward to measure the future midprice.

    Returns: Series of realized spread values (in price units).
    """
    if trade_df.empty or midprice_series.empty:
        return pd.Series(dtype=float)

    sign = trade_df["side"].map({"buy": 1, "sell": -1}).fillna(0)
    future_mid = midprice_series.shift(-horizon_steps)
    return sign * (trade_df["price"].values - future_mid.values)


def compute_vpin(trade_df: pd.DataFrame, bucket_size: float = 0.5) -> pd.Series:
    """Volume-synchronized PIN (VPIN) — a toxicity proxy.

    Divides the trade stream into volume buckets and measures the
    absolute buy/sell imbalance per bucket.

    Args:
        trade_df: DataFrame with columns [qty, side].
        bucket_size: Fraction of total volume per bucket (0 < x < 1).

    Returns: Series of VPIN values (one per bucket, indexed to bucket close row).
    """
    if trade_df.empty:
        return pd.Series(dtype=float)

    df = trade_df.copy().reset_index(drop=True)
    total_volume = df["qty"].sum()
    if total_volume == 0:
        return pd.Series(dtype=float)

    target_bucket_vol = total_volume * bucket_size

    vpins = []
    vpin_indices = []
    buy_vol = 0.0
    sell_vol = 0.0
    cum_vol = 0.0

    for idx, row in df.iterrows():
        if row["side"] == "buy":
            buy_vol += row["qty"]
        else:
            sell_vol += row["qty"]
        cum_vol += row["qty"]

        if cum_vol >= target_bucket_vol:
            bucket_vol = buy_vol + sell_vol
            vpin = abs(buy_vol - sell_vol) / bucket_vol if bucket_vol > 0 else 0.0
            vpins.append(vpin)
            vpin_indices.append(idx)
            buy_vol = 0.0
            sell_vol = 0.0
            cum_vol = 0.0

    return pd.Series(vpins, index=vpin_indices)


def compute_kyle_lambda(trade_df: pd.DataFrame) -> float:
    """Kyle's lambda: price impact coefficient via OLS regression.

    Regresses price change on signed volume: Δp = λ * signed_volume + ε
    Returns λ (price impact per unit of signed volume).
    """
    if len(trade_df) < 10:
        return float("nan")

    df = trade_df.copy().reset_index(drop=True)
    sign = df["side"].map({"buy": 1, "sell": -1}).fillna(0)
    signed_vol = sign * df["qty"]
    price_change = df["price"].diff()

    mask = price_change.notna() & signed_vol.notna()
    x = signed_vol[mask].values
    y = price_change[mask].values

    if len(x) < 2:
        return float("nan")

    # OLS: λ = Cov(y, x) / Var(x)
    cov = np.cov(y, x)
    if cov[1, 1] == 0:
        return float("nan")

    return float(cov[0, 1] / cov[1, 1])


def compute_rolling_features(
    trade_df: pd.DataFrame,
    book_df: pd.DataFrame | None = None,
    window: int = 100,
) -> pd.DataFrame:
    """Compute a bundle of rolling microstructure features.

    Returns a DataFrame aligned to trade_df with columns:
        - trade_imbalance
        - kyle_lambda_rolling
        - obi (if book_df provided)
    """
    features = pd.DataFrame(index=trade_df.index)
    features["trade_imbalance"] = compute_trade_imbalance(trade_df, window=window)

    if book_df is not None and not book_df.empty:
        # Align book OBI to trade timestamps via merge_asof
        book_obi = book_df[["timestamp_ns"]].copy()
        book_obi["obi"] = compute_obi(book_df)
        book_obi = book_obi.sort_values("timestamp_ns")

        trade_ts = trade_df[["timestamp_ns"]].copy().sort_values("timestamp_ns")
        merged = pd.merge_asof(trade_ts, book_obi, on="timestamp_ns", direction="backward")
        features["obi"] = merged["obi"].values

    return features
