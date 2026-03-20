"""
Layer 2 — Feature Store Service.

Precomputes and caches institutional-grade microstructure features from
Parquet data, serving them via the `/v1/features` API endpoint.

Features served:
    ┌─────────────────────────────────────────────────────────────────┐
    │  Flow Features                                                  │
    │    trade_imbalance     — rolling buy/sell volume ratio          │
    │    vpin                — volume-synced PIN (flow toxicity)      │
    │    kyle_lambda         — price impact coefficient               │
    │    realized_spread     — cost of immediacy proxy                │
    │                                                                 │
    │  Order Book Features                                            │
    │    obi_5               — top-5 order book imbalance            │
    │    obi_10              — top-10 order book imbalance           │
    │    spread_bps          — bid-ask spread in basis points         │
    │    depth_imbalance     — bid notional / ask notional (1%)       │
    │                                                                 │
    │  Volatility Features                                            │
    │    realised_vol_5m     — 5-min realised vol (annualised)        │
    │    realised_vol_1h     — 1-h realised vol (annualised)          │
    │    vol_ratio           — 5m vol / 1h vol (regime signal)        │
    │                                                                 │
    │  Price Features                                                 │
    │    momentum_5m         — 5-min log-return                       │
    │    momentum_1h         — 1-h log-return                         │
    │    vwap_1h             — volume-weighted average price (1h)     │
    │    price_vs_vwap       — (price - vwap) / vwap                  │
    └─────────────────────────────────────────────────────────────────┘

All features are computed in a sliding window and timestamped.
Results are cached in memory with a configurable TTL.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from features.microstructure import (
    compute_obi,
    compute_trade_imbalance,
    compute_vpin,
    compute_kyle_lambda,
    compute_realized_spread,
)


class FeatureStore:
    """
    Computes and caches microstructure feature bundles.

    Usage:
        store = FeatureStore(data_dir="./data")
        features = store.compute(exchange="binance", symbol="BTCUSDT",
                                 window_minutes=60)
    """

    CACHE_TTL_S = 30  # cached features expire after 30 seconds

    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)
        self._cache: dict[str, tuple[float, dict]] = {}  # key → (expiry, features)

    def compute(
        self,
        exchange: str,
        symbol: str,
        window_minutes: int = 60,
    ) -> dict:
        """
        Compute the full feature bundle for exchange/symbol over the last
        `window_minutes` of data.

        Returns a dict with feature values + metadata.
        """
        cache_key = f"{exchange}:{symbol}:{window_minutes}"
        cached = self._cache.get(cache_key)
        if cached and time.monotonic() < cached[0]:
            return cached[1]

        now  = datetime.now(timezone.utc)
        start = now - timedelta(minutes=window_minutes)
        result = self._compute_features(exchange, symbol, start, now, window_minutes)

        self._cache[cache_key] = (time.monotonic() + self.CACHE_TTL_S, result)
        return result

    def _compute_features(
        self,
        exchange: str,
        symbol: str,
        start: datetime,
        end: datetime,
        window_minutes: int,
    ) -> dict:
        from replay.engine import ReplayEngine  # lazy import

        engine = ReplayEngine(data_dir=str(self.data_dir))
        trade_df = engine._load("trades", exchange, symbol, start, end)
        book_df  = engine._load("books",  exchange, symbol, start, end)

        ts_ns = int(end.timestamp() * 1e9)

        features: dict = {
            "exchange":      exchange,
            "symbol":        symbol,
            "timestamp_ns":  ts_ns,
            "timestamp":     end.isoformat(),
            "window_minutes": window_minutes,
            "data_points": {
                "trades": len(trade_df),
                "books":  len(book_df),
            },
        }

        # ── Flow features ──────────────────────────────────────────────────
        if not trade_df.empty and len(trade_df) >= 5:
            imb_series = compute_trade_imbalance(trade_df, window=min(100, len(trade_df)))
            features["trade_imbalance"] = (
                round(float(imb_series.iloc[-1]), 6)
                if not imb_series.empty and not imb_series.isna().all()
                else None
            )

            vpin_series = compute_vpin(trade_df, bucket_size=0.5)
            features["vpin"] = (
                round(float(vpin_series.iloc[-1]), 6)
                if not vpin_series.empty
                else None
            )

            features["kyle_lambda"] = (
                round(compute_kyle_lambda(trade_df), 8)
                if len(trade_df) >= 10
                else None
            )

            # VWAP (1h)
            trade_df_vwap = trade_df.copy()
            trade_df_vwap["notional"] = trade_df_vwap["price"].astype(float) * trade_df_vwap["qty"].astype(float)
            total_notional = trade_df_vwap["notional"].sum()
            total_qty      = trade_df_vwap["qty"].astype(float).sum()
            vwap = float(total_notional / total_qty) if total_qty > 0 else None
            features["vwap"] = round(vwap, 6) if vwap else None

            last_price = float(trade_df["price"].iloc[-1]) if not trade_df.empty else None
            features["last_price"] = last_price
            features["price_vs_vwap"] = (
                round((last_price - vwap) / vwap, 6)
                if vwap and last_price
                else None
            )
        else:
            features.update({
                "trade_imbalance": None,
                "vpin": None,
                "kyle_lambda": None,
                "vwap": None,
                "last_price": None,
                "price_vs_vwap": None,
            })

        # ── Volatility features ────────────────────────────────────────────
        if not trade_df.empty and len(trade_df) >= 10:
            prices = trade_df["price"].astype(float).values
            log_ret = np.diff(np.log(np.maximum(prices, 1e-10)))

            if len(log_ret) >= 2:
                # 5-min window (recent trades)
                recent_n = min(len(log_ret), max(5, len(log_ret) // (window_minutes // 5 + 1)))
                vol_5m = float(np.std(log_ret[-recent_n:]) * np.sqrt(252 * 24 * 60))
                vol_1h = float(np.std(log_ret) * np.sqrt(252 * 24 * 60))
                features["realised_vol_short"] = round(vol_5m, 6)
                features["realised_vol_full"]  = round(vol_1h, 6)
                features["vol_ratio"]          = round(vol_5m / vol_1h, 4) if vol_1h > 0 else None
            else:
                features.update({"realised_vol_short": None, "realised_vol_full": None, "vol_ratio": None})

            # Momentum
            if len(prices) >= 2:
                momentum_full = float(np.log(prices[-1] / max(prices[0], 1e-10)))
                recent_p      = prices[-min(len(prices), max(5, len(prices) // (window_minutes // 5 + 1))):]
                momentum_short = float(np.log(recent_p[-1] / max(recent_p[0], 1e-10)))
                features["momentum_full"]  = round(momentum_full, 6)
                features["momentum_short"] = round(momentum_short, 6)
            else:
                features.update({"momentum_full": None, "momentum_short": None})
        else:
            features.update({
                "realised_vol_short": None, "realised_vol_full": None,
                "vol_ratio": None, "momentum_full": None, "momentum_short": None,
            })

        # ── Order book features ────────────────────────────────────────────
        if not book_df.empty:
            recent_book = book_df.tail(20)

            obi_series = compute_obi(recent_book, levels=5)
            features["obi_5"] = (
                round(float(obi_series.mean()), 6)
                if not obi_series.isna().all()
                else None
            )

            obi_10 = compute_obi(recent_book, levels=10)
            features["obi_10"] = (
                round(float(obi_10.mean()), 6)
                if not obi_10.isna().all()
                else None
            )

            if "spread" in recent_book.columns and "midprice" in recent_book.columns:
                mid  = recent_book["midprice"].replace("", np.nan).astype(float)
                sprd = recent_book["spread"].replace("", np.nan).astype(float)
                valid_mid = mid[mid > 0]
                if not valid_mid.empty:
                    features["spread_bps"] = round(float((sprd / mid * 10_000).mean()), 4)
                else:
                    features["spread_bps"] = None
            else:
                features["spread_bps"] = None
        else:
            features.update({"obi_5": None, "obi_10": None, "spread_bps": None})

        # ── Data quality ───────────────────────────────────────────────────
        features["data_quality"] = {
            "trade_coverage": "sufficient" if len(trade_df) >= 50 else "sparse",
            "book_coverage":  "sufficient" if len(book_df) >= 20 else "sparse",
            "window_complete": len(trade_df) >= 10 and len(book_df) >= 10,
        }

        return features
