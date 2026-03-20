"""
Layer 3 — Cross-Venue Arbitrage and Funding Rate Signal Detector.

Detects exploitable price dislocations across exchanges:

    1. Spot/futures basis arbitrage  — price gap between same instrument on different venues
    2. Triangular cross-venue arb    — three-leg price inconsistency
    3. Funding rate arbitrage        — positive/negative carry on perpetuals
    4. Liquidation-driven dislocation — temporary price gap caused by a cascade
    5. Mark price vs spot divergence  — index manipulation signal

Outputs ArbSignal objects ready for Layer 4 strategy consumption.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ArbType(str, Enum):
    CROSS_VENUE_PRICE   = "cross_venue_price"
    FUNDING_CARRY       = "funding_carry"
    MARK_SPOT_DIVERGENCE = "mark_spot_divergence"
    BASIS_MEAN_REVERSION = "basis_mean_reversion"


@dataclass
class ArbSignal:
    arb_type:       ArbType
    symbol:         str
    timestamp_ns:   int
    direction:      str            # "long_A_short_B" or "long_funding_short_spot" etc.
    edge_bps:       float          # estimated edge in basis points
    confidence:     float          # 0–1
    exchange_a:     str
    exchange_b:     str
    price_a:        float
    price_b:        float
    estimated_capacity_usd: float  # rough max position size
    metadata:       dict           = field(default_factory=dict)

    @property
    def timestamp_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            "arb_type":               self.arb_type.value,
            "symbol":                 self.symbol,
            "timestamp_ns":           self.timestamp_ns,
            "direction":              self.direction,
            "edge_bps":               round(self.edge_bps, 2),
            "confidence":             round(self.confidence, 4),
            "exchange_a":             self.exchange_a,
            "exchange_b":             self.exchange_b,
            "price_a":                self.price_a,
            "price_b":                self.price_b,
            "estimated_capacity_usd": round(self.estimated_capacity_usd, 2),
            "metadata":               self.metadata,
        }


class ArbDetector:
    """
    Cross-venue arbitrage and funding signal detector.

    Usage:
        detector = ArbDetector()
        signals = detector.detect_cross_venue(symbol, prices_by_exchange, books_by_exchange)
        signals += detector.detect_funding_carry(symbol, funding_by_exchange)
    """

    # Minimum edges to report (avoids noise)
    MIN_PRICE_ARB_BPS = 3.0    # 3 bps = min gross edge after fees
    MIN_FUNDING_APR   = 0.10   # 10% annualised funding carry
    MARK_SPOT_WARN_BPS = 5.0   # mark vs spot divergence threshold

    # Fee estimates (taker on each leg)
    TAKER_FEE_BPS = {
        "binance": 2.0,
        "bybit":   2.0,
        "okx":     2.0,
    }

    def detect_cross_venue(
        self,
        symbol: str,
        prices: dict[str, float],   # exchange → midprice
        spreads: dict[str, float],  # exchange → spread_bps
        timestamp_ns: int = 0,
    ) -> list[ArbSignal]:
        """
        Compare midprices across exchanges; flag if gap exceeds fees + min edge.
        """
        exchanges = list(prices.keys())
        signals: list[ArbSignal] = []

        for i, ex_a in enumerate(exchanges):
            for ex_b in exchanges[i+1:]:
                p_a = prices[ex_a]
                p_b = prices[ex_b]
                if p_a <= 0 or p_b <= 0:
                    continue

                # Raw price difference in bps
                raw_edge_bps = abs(p_a - p_b) / ((p_a + p_b) / 2) * 10_000

                # Deduct taker fees on both legs
                fee_a = self.TAKER_FEE_BPS.get(ex_a, 2.5)
                fee_b = self.TAKER_FEE_BPS.get(ex_b, 2.5)
                net_edge_bps = raw_edge_bps - fee_a - fee_b

                # Also deduct half the spread cost on each side
                spread_cost_bps = (
                    spreads.get(ex_a, 1.0) / 2 +
                    spreads.get(ex_b, 1.0) / 2
                )
                net_edge_bps -= spread_cost_bps

                if net_edge_bps < self.MIN_PRICE_ARB_BPS:
                    continue

                long_ex  = ex_a if p_a < p_b else ex_b
                short_ex = ex_b if p_a < p_b else ex_a
                long_p   = prices[long_ex]
                short_p  = prices[short_ex]

                # Capacity estimate: use minimum best-ask qty × price (proxy)
                capacity_usd = min(long_p, short_p) * 0.5  # conservative 0.5 BTC

                confidence = min(1.0, net_edge_bps / 20.0)

                signals.append(ArbSignal(
                    arb_type=ArbType.CROSS_VENUE_PRICE,
                    symbol=symbol,
                    timestamp_ns=timestamp_ns,
                    direction=f"long_{long_ex}_short_{short_ex}",
                    edge_bps=round(net_edge_bps, 3),
                    confidence=round(confidence, 4),
                    exchange_a=long_ex,
                    exchange_b=short_ex,
                    price_a=long_p,
                    price_b=short_p,
                    estimated_capacity_usd=capacity_usd,
                    metadata={
                        "raw_edge_bps":    round(raw_edge_bps, 3),
                        "fee_cost_bps":    round(fee_a + fee_b, 3),
                        "spread_cost_bps": round(spread_cost_bps, 3),
                    },
                ))

        return signals

    def detect_funding_carry(
        self,
        symbol: str,
        funding_rates: dict[str, float],  # exchange → 8h funding rate
        prices: dict[str, float],
        timestamp_ns: int = 0,
    ) -> list[ArbSignal]:
        """
        Detect funding rate arbitrage opportunities.

        Positive funding: longs pay shorts → short perp + long spot = carry income.
        Negative funding: shorts pay longs → long perp + short spot = carry income.
        """
        signals: list[ArbSignal] = []

        for exchange, rate_8h in funding_rates.items():
            if abs(rate_8h) < (self.MIN_FUNDING_APR / (3 * 365)):
                continue  # too small for 8h window

            # Annualise: 3 payments/day × 365 days
            apr = rate_8h * 3 * 365

            if abs(apr) < self.MIN_FUNDING_APR:
                continue

            direction = (
                "short_perp_long_spot" if rate_8h > 0   # longs pay → short perp
                else "long_perp_short_spot"              # shorts pay → long perp
            )

            edge_bps = abs(rate_8h) * 10_000  # bps per 8h window
            net_edge_bps = edge_bps - self.TAKER_FEE_BPS.get(exchange, 2.5) * 2

            if net_edge_bps < 1.0:
                continue

            price = prices.get(exchange, 0.0)
            signals.append(ArbSignal(
                arb_type=ArbType.FUNDING_CARRY,
                symbol=symbol,
                timestamp_ns=timestamp_ns,
                direction=direction,
                edge_bps=round(net_edge_bps, 3),
                confidence=min(1.0, abs(apr) / 1.0),
                exchange_a=exchange,
                exchange_b="spot",
                price_a=price,
                price_b=price,
                estimated_capacity_usd=price * 1.0,
                metadata={
                    "funding_rate_8h": round(rate_8h, 8),
                    "annualised_apr":  round(apr, 4),
                    "direction":       direction,
                },
            ))

        return signals

    def detect_mark_spot_divergence(
        self,
        symbol: str,
        mark_prices: dict[str, float],
        mid_prices: dict[str, float],
        timestamp_ns: int = 0,
    ) -> list[ArbSignal]:
        """
        Detect divergence between mark price and spot midprice.
        Large divergence can signal index manipulation or upcoming liquidation wave.
        """
        signals: list[ArbSignal] = []

        for exchange in mark_prices:
            mark = mark_prices.get(exchange)
            mid  = mid_prices.get(exchange)
            if not mark or not mid or mid <= 0:
                continue

            divergence_bps = abs(mark - mid) / mid * 10_000

            if divergence_bps < self.MARK_SPOT_WARN_BPS:
                continue

            direction = "mark_above_spot" if mark > mid else "mark_below_spot"
            signals.append(ArbSignal(
                arb_type=ArbType.MARK_SPOT_DIVERGENCE,
                symbol=symbol,
                timestamp_ns=timestamp_ns,
                direction=direction,
                edge_bps=round(divergence_bps, 2),
                confidence=min(1.0, divergence_bps / 50.0),
                exchange_a=exchange,
                exchange_b=exchange,
                price_a=mark,
                price_b=mid,
                estimated_capacity_usd=0.0,
                metadata={
                    "mark_price":     mark,
                    "mid_price":      mid,
                    "divergence_bps": round(divergence_bps, 2),
                    "signal":         "index_manipulation_risk" if divergence_bps > 20 else "elevated_divergence",
                },
            ))

        return signals
