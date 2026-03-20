"""
Layer 3 — Market Regime Detector.

Classifies the current market microstructure regime using multiple signals:

    1. Price momentum        — directional trend strength
    2. Realised volatility   — rolling std of log-returns (normalised)
    3. Liquidity regime      — bid-ask spread and OBI stability
    4. Trade flow balance    — buy/sell volume asymmetry
    5. Structural break      — CUSUM-style change-point detection

Regimes:
    TRENDING_BULL   high momentum, low vol, buy-dominated flow
    TRENDING_BEAR   high negative momentum, low vol, sell-dominated flow
    CHOPPY          low momentum, low vol, balanced flow
    VOLATILE        high vol, wide spreads, erratic flow
    CRISIS          extreme vol + liquidation cascade + OI collapse

Output is a RegimeSnapshot used by downstream intelligence and strategy layers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd


class Regime(str, Enum):
    TRENDING_BULL = "trending_bull"
    TRENDING_BEAR = "trending_bear"
    CHOPPY        = "choppy"
    VOLATILE      = "volatile"
    CRISIS        = "crisis"
    UNKNOWN       = "unknown"


@dataclass
class RegimeSnapshot:
    exchange:       str
    symbol:         str
    timestamp_ns:   int
    regime:         Regime
    confidence:     float          # 0–1
    momentum:       float          # signed, normalised
    realised_vol:   float          # annualised proxy
    spread_bps:     Optional[float]
    obi:            Optional[float]
    flow_imbalance: float          # buy_vol / total_vol
    structural_break: bool
    signals:        dict           = field(default_factory=dict)

    @property
    def timestamp_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            "exchange":         self.exchange,
            "symbol":           self.symbol,
            "timestamp_ns":     self.timestamp_ns,
            "regime":           self.regime.value,
            "confidence":       round(self.confidence, 4),
            "momentum":         round(self.momentum, 6),
            "realised_vol":     round(self.realised_vol, 6),
            "spread_bps":       round(self.spread_bps, 4) if self.spread_bps is not None else None,
            "obi":              round(self.obi, 4) if self.obi is not None else None,
            "flow_imbalance":   round(self.flow_imbalance, 4),
            "structural_break": self.structural_break,
            "signals":          self.signals,
        }


class RegimeDetector:
    """
    Stateless regime classifier — accepts DataFrames and returns a RegimeSnapshot.

    Usage:
        detector = RegimeDetector()
        snap = detector.detect(exchange, symbol, trade_df, book_df)
    """

    # Thresholds
    MOMENTUM_TREND_THRESH  = 0.003   # 0.3% log-return momentum = directional
    VOL_VOLATILE_THRESH    = 1.20    # annualised vol > 120% = volatile
    VOL_CRISIS_THRESH      = 2.50    # > 250% = crisis territory
    SPREAD_CRISIS_BPS      = 15.0    # spread > 15 bps = liquidity stress
    FLOW_IMBALANCE_THRESH  = 0.65    # > 65% one-sided = flow-driven
    CUSUM_THRESH           = 4.0     # standard deviations

    def detect(
        self,
        exchange: str,
        symbol: str,
        trade_df: pd.DataFrame,
        book_df: pd.DataFrame | None = None,
        window: int = 300,
    ) -> RegimeSnapshot:
        """
        Classify regime from recent trade and book data.

        Args:
            trade_df:  DataFrame with columns [timestamp_ns, price, qty, side].
                       Expected to cover the last `window` trades or recent minutes.
            book_df:   Optional DataFrame with [timestamp_ns, spread, obi_5].
            window:    Number of recent trades to use.

        Returns:
            RegimeSnapshot
        """
        ts_now = int(trade_df["timestamp_ns"].iloc[-1]) if not trade_df.empty else 0
        df = trade_df.tail(window).copy() if len(trade_df) > window else trade_df.copy()

        momentum       = self._momentum(df)
        realised_vol   = self._realised_vol(df)
        flow_imbalance = self._flow_imbalance(df)
        structural_brk = self._structural_break(df)

        spread_bps, obi = None, None
        if book_df is not None and not book_df.empty:
            recent_book = book_df.tail(20)
            if "spread" in recent_book.columns and "midprice" in recent_book.columns:
                mid  = recent_book["midprice"].replace("", np.nan).astype(float)
                sprd = recent_book["spread"].replace("", np.nan).astype(float)
                valid = mid[mid > 0]
                if not valid.empty:
                    spread_bps = float((sprd / mid * 10_000).mean())
            if "obi_5" in recent_book.columns:
                obi_vals = recent_book["obi_5"].replace("", np.nan).astype(float)
                obi = float(obi_vals.mean()) if not obi_vals.isna().all() else None

        regime, confidence = self._classify(
            momentum, realised_vol, flow_imbalance, spread_bps, structural_brk
        )

        signals = {
            "momentum_signal":    "bull" if momentum > self.MOMENTUM_TREND_THRESH else
                                  "bear" if momentum < -self.MOMENTUM_TREND_THRESH else "neutral",
            "vol_signal":         "crisis" if realised_vol > self.VOL_CRISIS_THRESH else
                                  "volatile" if realised_vol > self.VOL_VOLATILE_THRESH else "normal",
            "flow_signal":        "buy_dominant"  if flow_imbalance > self.FLOW_IMBALANCE_THRESH else
                                  "sell_dominant" if flow_imbalance < (1 - self.FLOW_IMBALANCE_THRESH) else "balanced",
            "spread_signal":      "stressed" if spread_bps and spread_bps > self.SPREAD_CRISIS_BPS else "normal",
            "structural_break":   structural_brk,
        }

        return RegimeSnapshot(
            exchange=exchange,
            symbol=symbol,
            timestamp_ns=ts_now,
            regime=regime,
            confidence=confidence,
            momentum=momentum,
            realised_vol=realised_vol,
            spread_bps=spread_bps,
            obi=obi,
            flow_imbalance=flow_imbalance,
            structural_break=structural_brk,
            signals=signals,
        )

    # ── Signal computation ────────────────────────────────────────────────────

    def _momentum(self, df: pd.DataFrame) -> float:
        """Log-return momentum: log(last_price / first_price)."""
        if len(df) < 2:
            return 0.0
        prices = df["price"].astype(float)
        first, last = prices.iloc[0], prices.iloc[-1]
        if first <= 0:
            return 0.0
        return float(np.log(last / first))

    def _realised_vol(self, df: pd.DataFrame, annualise_factor: float = 252 * 24 * 60) -> float:
        """
        Realised volatility from log-returns.
        annualise_factor: converts per-trade vol to annualised (assumes ~1 trade/min avg).
        """
        if len(df) < 10:
            return 0.0
        prices = df["price"].astype(float).values
        log_returns = np.diff(np.log(prices[prices > 0]))
        if len(log_returns) < 2:
            return 0.0
        return float(np.std(log_returns) * np.sqrt(annualise_factor))

    def _flow_imbalance(self, df: pd.DataFrame) -> float:
        """Buy volume fraction: buy_vol / (buy_vol + sell_vol)."""
        if df.empty or "side" not in df.columns:
            return 0.5
        buy_vol  = df.loc[df["side"] == "buy",  "qty"].astype(float).sum()
        sell_vol = df.loc[df["side"] == "sell", "qty"].astype(float).sum()
        total = buy_vol + sell_vol
        return float(buy_vol / total) if total > 0 else 0.5

    def _structural_break(self, df: pd.DataFrame) -> bool:
        """
        CUSUM change-point detector on log-returns.
        Returns True if cumulative sum exceeds CUSUM_THRESH standard deviations.
        """
        if len(df) < 20:
            return False
        prices = df["price"].astype(float).values
        log_returns = np.diff(np.log(np.maximum(prices, 1e-10)))
        if len(log_returns) < 5:
            return False
        mu  = np.mean(log_returns)
        std = np.std(log_returns) + 1e-12
        cusum    = np.cumsum((log_returns - mu) / std)
        cusum_lo = np.cumsum((mu - log_returns) / std)
        return bool(np.max(np.abs(cusum)) > self.CUSUM_THRESH or
                    np.max(cusum_lo) > self.CUSUM_THRESH)

    # ── Classification ────────────────────────────────────────────────────────

    def _classify(
        self,
        momentum: float,
        realised_vol: float,
        flow_imbalance: float,
        spread_bps: float | None,
        structural_break: bool,
    ) -> tuple[Regime, float]:
        """
        Vote-based regime classification with confidence score.
        Each condition casts a vote; the plurality wins.
        """
        votes: dict[Regime, float] = {r: 0.0 for r in Regime}

        # Crisis: extreme vol OR extreme spread
        if realised_vol > self.VOL_CRISIS_THRESH:
            votes[Regime.CRISIS] += 3.0
        if spread_bps and spread_bps > self.SPREAD_CRISIS_BPS:
            votes[Regime.CRISIS] += 2.0
        if structural_break and realised_vol > self.VOL_VOLATILE_THRESH:
            votes[Regime.CRISIS] += 1.0

        # Volatile: elevated vol
        if self.VOL_VOLATILE_THRESH < realised_vol <= self.VOL_CRISIS_THRESH:
            votes[Regime.VOLATILE] += 3.0
        if structural_break:
            votes[Regime.VOLATILE] += 1.0

        # Trending bull
        if momentum > self.MOMENTUM_TREND_THRESH and realised_vol < self.VOL_VOLATILE_THRESH:
            votes[Regime.TRENDING_BULL] += 2.0
        if flow_imbalance > self.FLOW_IMBALANCE_THRESH:
            votes[Regime.TRENDING_BULL] += 1.5

        # Trending bear
        if momentum < -self.MOMENTUM_TREND_THRESH and realised_vol < self.VOL_VOLATILE_THRESH:
            votes[Regime.TRENDING_BEAR] += 2.0
        if flow_imbalance < (1 - self.FLOW_IMBALANCE_THRESH):
            votes[Regime.TRENDING_BEAR] += 1.5

        # Choppy: low vol, low momentum, balanced flow
        if (abs(momentum) < self.MOMENTUM_TREND_THRESH and
                realised_vol < self.VOL_VOLATILE_THRESH and
                abs(flow_imbalance - 0.5) < 0.15):
            votes[Regime.CHOPPY] += 3.0

        # Pick winner
        best_regime = max(votes, key=lambda r: votes[r])
        best_votes  = votes[best_regime]
        total_votes = sum(votes.values()) + 1e-9

        if best_votes < 1.0:
            return Regime.UNKNOWN, 0.0

        confidence = min(1.0, best_votes / total_votes * 2.5)
        return best_regime, round(confidence, 4)
