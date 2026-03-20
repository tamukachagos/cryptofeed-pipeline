"""L2 Order Book Engine using SortedContainers.

Maintains best bid/ask, spread, midprice, microprice, and OBI.
Detects sequence gaps and flags when REST resync is needed.
"""
from __future__ import annotations

from sortedcontainers import SortedDict


class OrderBookEngine:
    """L2 order book with gap detection and microstructure metrics.

    Bids: SortedDict with negated keys so index 0 = best (highest) bid.
    Asks: SortedDict with natural keys so index 0 = best (lowest) ask.
    """

    def __init__(self, symbol: str, exchange: str, depth: int = 50) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self.depth = depth
        # Negated key → qty for bids (highest price first)
        self._bids: SortedDict = SortedDict()
        # Natural key → qty for asks (lowest price first)
        self._asks: SortedDict = SortedDict()
        self._last_seq: int = -1
        self._initialized: bool = False
        self._gap_count: int = 0

    # ------------------------------------------------------------------
    # Public mutations
    # ------------------------------------------------------------------

    def apply_snapshot(self, bids: list, asks: list, seq: int = -1) -> None:
        """Full book reset from a snapshot message."""
        self._bids.clear()
        self._asks.clear()
        for level in bids:
            price, qty = float(level[0]), float(level[1])
            if qty > 0:
                self._bids[-price] = qty
        for level in asks:
            price, qty = float(level[0]), float(level[1])
            if qty > 0:
                self._asks[price] = qty
        self._last_seq = seq
        self._initialized = True

        # Validate: crossed book (best_bid >= best_ask) is invalid — clear and warn
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba and bb[0] >= ba[0]:
            from loguru import logger
            logger.warning(
                f"[{self.exchange}:{self.symbol}] Crossed book in snapshot "
                f"(best_bid={bb[0]} >= best_ask={ba[0]}) — discarding snapshot"
            )
            self._bids.clear()
            self._asks.clear()
            self._initialized = False

    def apply_delta(self, bids: list, asks: list, seq: int = -1) -> bool:
        """Apply incremental update. Returns False if sequence gap detected."""
        if not self._initialized:
            return False

        if seq != -1 and self._last_seq != -1 and seq != self._last_seq + 1:
            self._gap_count += 1
            return False

        for level in bids:
            price, qty = float(level[0]), float(level[1])
            neg_p = -price
            if qty == 0:
                self._bids.pop(neg_p, None)
            else:
                self._bids[neg_p] = qty

        for level in asks:
            price, qty = float(level[0]), float(level[1])
            if qty == 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty

        self._last_seq = seq
        return True

    # ------------------------------------------------------------------
    # Public reads
    # ------------------------------------------------------------------

    def best_bid(self) -> tuple[float, float] | None:
        if not self._bids:
            return None
        neg_p, qty = self._bids.peekitem(0)
        return -neg_p, qty

    def best_ask(self) -> tuple[float, float] | None:
        if not self._asks:
            return None
        p, qty = self._asks.peekitem(0)
        return p, qty

    def spread(self) -> float | None:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            return ba[0] - bb[0]
        return None

    def midprice(self) -> float | None:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            return (bb[0] + ba[0]) / 2.0
        return None

    def microprice(self) -> float | None:
        """Volume-weighted midprice: weights each side by the other side's qty."""
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            total_qty = bb[1] + ba[1]
            if total_qty == 0:
                return None
            return (bb[0] * ba[1] + ba[0] * bb[1]) / total_qty
        return None

    def order_book_imbalance(self, levels: int = 5) -> float | None:
        """OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol) for top N price levels."""
        bid_items = list(self._bids.values())[:levels]
        ask_items = list(self._asks.values())[:levels]
        bid_vol = sum(bid_items)
        ask_vol = sum(ask_items)
        total = bid_vol + ask_vol
        if total == 0:
            return None
        return (bid_vol - ask_vol) / total

    def snapshot(self, levels: int = 20) -> dict:
        """Export top N levels as a serializable dict."""
        bids = [[-neg_p, qty] for neg_p, qty in list(self._bids.items())[:levels]]
        asks = [[p, qty] for p, qty in list(self._asks.items())[:levels]]
        return {
            "bids": bids,
            "asks": asks,
            "seq": self._last_seq,
            "gap_count": self._gap_count,
            "spread": self.spread(),
            "midprice": self.midprice(),
            "microprice": self.microprice(),
            "obi_5": self.order_book_imbalance(5),
        }
