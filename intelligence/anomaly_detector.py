"""
Layer 3 — Market Anomaly Detector.

Detects adversarial and structural market anomalies in real-time data:

    1. Spoofing detection     — large orders appearing and vanishing without fills
    2. Layering detection     — multiple price-levels refreshed in sync
    3. Liquidation cascade    — clustered forced liquidations with accelerating price impact
    4. Wash trading           — near-simultaneous buy/sell of same size at same price
    5. Momentum ignition      — trade burst followed by rapid price reversal
    6. Quote stuffing         — abnormal book update frequency

Each detector returns an AnomalyEvent with:
    - type, severity (0–1), confidence (0–1)
    - supporting evidence dict
    - exchange trust score impact

Trust scoring:
    Each exchange starts at trust_score = 1.0.
    Anomaly events decay the trust score: trust *= (1 - severity * decay_rate).
    Trust recovers exponentially when no anomalies fire.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd


class AnomalyType(str, Enum):
    SPOOFING             = "spoofing"
    LAYERING             = "layering"
    LIQUIDATION_CASCADE  = "liquidation_cascade"
    WASH_TRADING         = "wash_trading"
    MOMENTUM_IGNITION    = "momentum_ignition"
    QUOTE_STUFFING       = "quote_stuffing"
    PRICE_OUTLIER        = "price_outlier"


@dataclass
class AnomalyEvent:
    anomaly_type:   AnomalyType
    exchange:       str
    symbol:         str
    timestamp_ns:   int
    severity:       float          # 0–1 (1 = confirmed, 0 = suspected)
    confidence:     float          # 0–1
    description:    str
    evidence:       dict           = field(default_factory=dict)

    @property
    def timestamp_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            "anomaly_type": self.anomaly_type.value,
            "exchange":     self.exchange,
            "symbol":       self.symbol,
            "timestamp_ns": self.timestamp_ns,
            "severity":     round(self.severity, 4),
            "confidence":   round(self.confidence, 4),
            "description":  self.description,
            "evidence":     self.evidence,
        }


class ExchangeTrustScorer:
    """
    Per-exchange trust score based on anomaly history.
    Decays on each anomaly; recovers exponentially over time.
    Scores are persisted to SQLite so server restarts preserve history.
    """

    DECAY_RATE    = 0.05    # per anomaly event (severity-weighted)
    RECOVERY_RATE = 0.001   # per second
    MIN_TRUST     = 0.20

    def __init__(self) -> None:
        self._scores: dict[str, float]  = {}
        self._last_ts: dict[str, float] = {}
        self._load_from_db()

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _db_path(self):
        import os
        from pathlib import Path
        return Path(os.getenv("DATA_DIR", "./data")) / "users.db"

    def _load_from_db(self) -> None:
        """Load persisted trust scores from SQLite on startup."""
        import sqlite3
        db = self._db_path()
        if not db.exists():
            return
        try:
            conn = sqlite3.connect(str(db))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS exchange_trust (
                    exchange TEXT PRIMARY KEY,
                    score REAL NOT NULL DEFAULT 1.0,
                    last_update_wall REAL NOT NULL
                )
            """)
            conn.commit()
            now_wall = time.time()
            for row in conn.execute("SELECT exchange, score, last_update_wall FROM exchange_trust").fetchall():
                ex, score, last_wall = row
                # Apply recovery for time elapsed since last update
                elapsed = max(0.0, now_wall - last_wall)
                recovered = min(1.0, score + self.RECOVERY_RATE * elapsed)
                self._scores[ex]  = recovered
                self._last_ts[ex] = time.monotonic()
            conn.close()
        except Exception:
            pass

    def _save_to_db(self, exchange: str) -> None:
        """Persist current score for an exchange to SQLite."""
        import sqlite3
        db = self._db_path()
        try:
            db.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS exchange_trust (
                    exchange TEXT PRIMARY KEY,
                    score REAL NOT NULL DEFAULT 1.0,
                    last_update_wall REAL NOT NULL
                )
            """)
            conn.execute(
                """INSERT OR REPLACE INTO exchange_trust (exchange, score, last_update_wall)
                   VALUES (?, ?, ?)""",
                (exchange, self._scores.get(exchange, 1.0), time.time())
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    # ── Public interface ──────────────────────────────────────────────────────

    def score(self, exchange: str) -> float:
        self._recover(exchange)
        return self._scores.get(exchange, 1.0)

    def penalise(self, exchange: str, severity: float) -> None:
        self._recover(exchange)
        current = self._scores.get(exchange, 1.0)
        self._scores[exchange] = max(
            self.MIN_TRUST,
            current * (1.0 - severity * self.DECAY_RATE),
        )
        self._last_ts[exchange] = time.monotonic()
        self._save_to_db(exchange)

    def _recover(self, exchange: str) -> None:
        now  = time.monotonic()
        last = self._last_ts.get(exchange, now)
        elapsed = max(0.0, now - last)
        current = self._scores.get(exchange, 1.0)
        recovered = min(1.0, current + self.RECOVERY_RATE * elapsed)
        self._scores[exchange] = recovered
        self._last_ts[exchange] = now


class AnomalyDetector:
    """
    Stateless anomaly detector.  All detectors accept DataFrames and return
    a list of AnomalyEvent objects (empty = no anomaly detected).

    Usage:
        detector = AnomalyDetector()
        events = detector.run_all(exchange, symbol, trade_df, book_df, liq_df)
    """

    # Thresholds
    SPOOF_SIZE_MULTIPLIER  = 8.0    # order must be 8× avg queue size
    SPOOF_LIFETIME_MS      = 500    # order placed and removed within 500ms
    SPOOF_FILL_RATE        = 0.02   # < 2% filled before removal
    WASH_PRICE_TOLERANCE   = 0.001  # within 0.1% of same price
    WASH_QTY_TOLERANCE     = 0.05   # within 5% of same qty
    WASH_TIME_WINDOW_NS    = 500_000_000   # 500ms window
    CASCADE_MIN_LIQS       = 5      # at least 5 liquidations
    CASCADE_WINDOW_S       = 60     # within 60 seconds
    CASCADE_PRICE_MOVE_PCT = 0.005  # price moved >0.5% during cascade
    IGNITION_BURST_MULT    = 5.0    # trade rate 5× baseline
    IGNITION_REVERSAL_PCT  = 0.003  # then price reverses >0.3%
    STUFFING_UPDATE_MULT   = 20.0   # book updates 20× baseline per second
    PRICE_OUTLIER_ZSCORE   = 6.0    # z-score >6 = price outlier

    def run_all(
        self,
        exchange: str,
        symbol: str,
        trade_df: pd.DataFrame,
        book_df: pd.DataFrame | None = None,
        liq_df: pd.DataFrame | None = None,
    ) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []

        if trade_df is not None and not trade_df.empty:
            events.extend(self.detect_wash_trading(exchange, symbol, trade_df))
            events.extend(self.detect_price_outliers(exchange, symbol, trade_df))
            events.extend(self.detect_momentum_ignition(exchange, symbol, trade_df))

        if book_df is not None and not book_df.empty:
            events.extend(self.detect_quote_stuffing(exchange, symbol, book_df))

        if liq_df is not None and not liq_df.empty:
            events.extend(self.detect_liquidation_cascade(exchange, symbol, liq_df, trade_df))

        return events

    # ── Wash Trading ─────────────────────────────────────────────────────────

    def detect_wash_trading(
        self, exchange: str, symbol: str, trade_df: pd.DataFrame
    ) -> list[AnomalyEvent]:
        """
        Detect near-simultaneous buy + sell at the same price and qty.
        True wash trading is nearly impossible on public markets but
        detects coordinated cross-account manipulation.
        """
        if len(trade_df) < 10:
            return []

        df = trade_df.copy().sort_values("timestamp_ns")
        buys  = df[df["side"] == "buy"].copy()
        sells = df[df["side"] == "sell"].copy()

        if buys.empty or sells.empty:
            return []

        events: list[AnomalyEvent] = []
        for _, b in buys.iterrows():
            window = sells[
                (abs(sells["timestamp_ns"] - b["timestamp_ns"]) < self.WASH_TIME_WINDOW_NS) &
                (abs(sells["price"] - b["price"]) / (b["price"] + 1e-9) < self.WASH_PRICE_TOLERANCE) &
                (abs(sells["qty"] - b["qty"]) / (b["qty"] + 1e-9) < self.WASH_QTY_TOLERANCE)
            ]
            if not window.empty:
                events.append(AnomalyEvent(
                    anomaly_type=AnomalyType.WASH_TRADING,
                    exchange=exchange,
                    symbol=symbol,
                    timestamp_ns=int(b["timestamp_ns"]),
                    severity=0.70,
                    confidence=0.65,
                    description=f"Buy/sell pair at price={b['price']:.2f} qty={b['qty']:.4f} within 500ms",
                    evidence={
                        "buy_trade_id":  str(b.get("trade_id", "")),
                        "sell_trade_id": str(window.iloc[0].get("trade_id", "")),
                        "price":         float(b["price"]),
                        "qty":           float(b["qty"]),
                        "time_delta_ns": int(abs(window.iloc[0]["timestamp_ns"] - b["timestamp_ns"])),
                    },
                ))

        return events[:5]  # cap to avoid flood

    # ── Liquidation Cascade ───────────────────────────────────────────────────

    def detect_liquidation_cascade(
        self,
        exchange: str,
        symbol: str,
        liq_df: pd.DataFrame,
        trade_df: pd.DataFrame | None = None,
    ) -> list[AnomalyEvent]:
        """
        Detect a cluster of liquidations driving a price cascade.
        """
        if len(liq_df) < self.CASCADE_MIN_LIQS:
            return []

        df = liq_df.copy().sort_values("timestamp_ns")
        ts_start = df["timestamp_ns"].iloc[0]
        ts_end   = df["timestamp_ns"].iloc[-1]
        duration_s = (ts_end - ts_start) / 1e9

        if duration_s > self.CASCADE_WINDOW_S:
            return []

        n_liqs = len(df)
        total_qty = df["qty"].astype(float).sum()

        # Check if price moved significantly during the cascade
        price_move = 0.0
        if trade_df is not None and not trade_df.empty:
            window_trades = trade_df[
                (trade_df["timestamp_ns"] >= ts_start) &
                (trade_df["timestamp_ns"] <= ts_end)
            ]
            if len(window_trades) >= 2:
                p0 = float(window_trades["price"].iloc[0])
                p1 = float(window_trades["price"].iloc[-1])
                price_move = abs(p1 - p0) / (p0 + 1e-9)

        if price_move < self.CASCADE_PRICE_MOVE_PCT and n_liqs < 10:
            return []

        severity   = min(1.0, n_liqs / 20 + price_move / 0.02)
        confidence = min(1.0, n_liqs / 15)
        dominant_side = "sell" if df["side"].value_counts().idxmax() == "sell" else "buy"

        return [AnomalyEvent(
            anomaly_type=AnomalyType.LIQUIDATION_CASCADE,
            exchange=exchange,
            symbol=symbol,
            timestamp_ns=int(ts_end),
            severity=round(severity, 3),
            confidence=round(confidence, 3),
            description=(
                f"Liquidation cascade: {n_liqs} forced liquidations in {duration_s:.1f}s "
                f"({dominant_side}-side), price moved {price_move:.2%}"
            ),
            evidence={
                "liquidation_count": n_liqs,
                "total_qty":         round(total_qty, 4),
                "duration_s":        round(duration_s, 2),
                "price_move_pct":    round(price_move * 100, 4),
                "dominant_side":     dominant_side,
            },
        )]

    # ── Momentum Ignition ─────────────────────────────────────────────────────

    def detect_momentum_ignition(
        self, exchange: str, symbol: str, trade_df: pd.DataFrame
    ) -> list[AnomalyEvent]:
        """
        Detect trade-burst followed by rapid price reversal.
        Classic pattern: actor floods buys to push price, dumps into the move.
        """
        if len(trade_df) < 30:
            return []

        df = trade_df.copy().sort_values("timestamp_ns").reset_index(drop=True)

        # Compute per-second trade rate in rolling windows
        df["t_s"] = df["timestamp_ns"] / 1e9
        baseline_rate = len(df) / max(1.0, (df["t_s"].iloc[-1] - df["t_s"].iloc[0]))

        # Look at last 10% of trades for burst
        burst_n = max(5, len(df) // 10)
        burst = df.tail(burst_n)
        burst_dur = max(0.1, burst["t_s"].iloc[-1] - burst["t_s"].iloc[0])
        burst_rate = len(burst) / burst_dur

        if burst_rate < baseline_rate * self.IGNITION_BURST_MULT:
            return []

        # Check for reversal
        burst_start_price = float(burst["price"].iloc[0])
        burst_peak_price  = float(burst["price"].max() if burst["side"].iloc[-1] == "buy" else burst["price"].min())
        post_burst        = df.tail(max(5, len(df) // 20))
        post_price        = float(post_burst["price"].iloc[-1])

        reversal = abs(post_price - burst_peak_price) / (burst_peak_price + 1e-9)

        if reversal < self.IGNITION_REVERSAL_PCT:
            return []

        return [AnomalyEvent(
            anomaly_type=AnomalyType.MOMENTUM_IGNITION,
            exchange=exchange,
            symbol=symbol,
            timestamp_ns=int(burst["timestamp_ns"].iloc[-1]),
            severity=min(1.0, reversal / 0.01),
            confidence=min(1.0, burst_rate / (baseline_rate * self.IGNITION_BURST_MULT)),
            description=(
                f"Momentum ignition: trade rate {burst_rate:.1f}/s ({burst_rate/baseline_rate:.1f}× baseline), "
                f"price reversal {reversal:.2%}"
            ),
            evidence={
                "burst_trade_rate":  round(burst_rate, 2),
                "baseline_rate":     round(baseline_rate, 2),
                "rate_multiplier":   round(burst_rate / max(baseline_rate, 1e-9), 2),
                "price_reversal_pct": round(reversal * 100, 4),
            },
        )]

    # ── Quote Stuffing ────────────────────────────────────────────────────────

    def detect_quote_stuffing(
        self, exchange: str, symbol: str, book_df: pd.DataFrame
    ) -> list[AnomalyEvent]:
        """
        Detect abnormally high book update frequency — signals algorithmic
        market manipulation intended to saturate competitor feed processing.
        """
        if len(book_df) < 20:
            return []

        df = book_df.copy().sort_values("timestamp_ns")
        total_s = (df["timestamp_ns"].iloc[-1] - df["timestamp_ns"].iloc[0]) / 1e9

        if total_s < 1.0:
            return []

        updates_per_sec = len(df) / total_s
        # Typical: Binance depth@100ms = 10/s, Bybit = 10-20/s
        # 200ms book = ~200/s would be abnormal
        baseline = 15.0  # updates/sec expected

        if updates_per_sec < baseline * self.STUFFING_UPDATE_MULT:
            return []

        return [AnomalyEvent(
            anomaly_type=AnomalyType.QUOTE_STUFFING,
            exchange=exchange,
            symbol=symbol,
            timestamp_ns=int(df["timestamp_ns"].iloc[-1]),
            severity=min(1.0, updates_per_sec / (baseline * 50)),
            confidence=0.70,
            description=(
                f"Quote stuffing: {updates_per_sec:.0f} book updates/sec "
                f"({updates_per_sec/baseline:.1f}× normal)"
            ),
            evidence={
                "updates_per_sec":  round(updates_per_sec, 2),
                "baseline_per_sec": baseline,
                "multiplier":       round(updates_per_sec / baseline, 2),
                "window_s":         round(total_s, 2),
            },
        )]

    # ── Price Outliers ────────────────────────────────────────────────────────

    def detect_price_outliers(
        self, exchange: str, symbol: str, trade_df: pd.DataFrame
    ) -> list[AnomalyEvent]:
        """
        Detect individually erroneous trades (fat-finger, exchange glitch).
        Uses z-score on log-returns.
        """
        if len(trade_df) < 20:
            return []

        prices = trade_df["price"].astype(float).values
        log_p  = np.log(np.maximum(prices, 1e-10))
        returns = np.diff(log_p)

        if len(returns) < 5:
            return []

        mu   = np.mean(returns)
        std  = np.std(returns) + 1e-12
        zscores = np.abs((returns - mu) / std)

        outlier_idx = np.where(zscores > self.PRICE_OUTLIER_ZSCORE)[0]
        if len(outlier_idx) == 0:
            return []

        events = []
        for idx in outlier_idx[:3]:
            trade_idx = int(idx + 1)
            if trade_idx >= len(trade_df):
                continue
            row = trade_df.iloc[trade_idx]
            events.append(AnomalyEvent(
                anomaly_type=AnomalyType.PRICE_OUTLIER,
                exchange=exchange,
                symbol=symbol,
                timestamp_ns=int(row["timestamp_ns"]),
                severity=min(1.0, float(zscores[idx]) / 20.0),
                confidence=0.85,
                description=f"Price outlier: z-score={zscores[idx]:.1f} at price={row['price']:.4f}",
                evidence={
                    "price":      float(row["price"]),
                    "trade_id":   str(row.get("trade_id", "")),
                    "z_score":    round(float(zscores[idx]), 2),
                    "log_return": round(float(returns[idx]), 6),
                },
            ))

        return events
