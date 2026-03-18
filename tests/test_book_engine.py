"""Unit tests for OrderBookEngine."""
import pytest
from collector.book_engine import OrderBookEngine


@pytest.fixture
def book():
    return OrderBookEngine("BTCUSDT", "test")


def _snap_bids():
    return [["65000.0", "1.5"], ["64999.0", "2.0"], ["64998.0", "3.0"]]


def _snap_asks():
    return [["65001.0", "1.0"], ["65002.0", "2.5"], ["65003.0", "4.0"]]


class TestSnapshot:
    def test_snapshot_initializes_book(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=100)
        assert book._initialized
        assert book._last_seq == 100

    def test_best_bid_after_snapshot(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks())
        bb = book.best_bid()
        assert bb is not None
        assert bb[0] == pytest.approx(65000.0)
        assert bb[1] == pytest.approx(1.5)

    def test_best_ask_after_snapshot(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks())
        ba = book.best_ask()
        assert ba is not None
        assert ba[0] == pytest.approx(65001.0)
        assert ba[1] == pytest.approx(1.0)

    def test_spread(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks())
        assert book.spread() == pytest.approx(1.0)

    def test_midprice(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks())
        assert book.midprice() == pytest.approx(65000.5)

    def test_microprice(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks())
        mp = book.microprice()
        assert mp is not None
        # Bid qty 1.5 > ask qty 1.0 → microprice > midprice (closer to bid)
        assert mp > 65000.0
        assert mp < 65001.0

    def test_obi_positive_when_more_bids(self, book):
        # 1.5 bids vs 1.0 asks at best level
        book.apply_snapshot(_snap_bids(), _snap_asks())
        obi = book.order_book_imbalance(1)
        assert obi is not None
        assert obi > 0  # more bid volume

    def test_empty_book_returns_none(self, book):
        assert book.best_bid() is None
        assert book.best_ask() is None
        assert book.spread() is None
        assert book.midprice() is None
        assert book.microprice() is None
        assert book.order_book_imbalance() is None


class TestDelta:
    def test_delta_updates_qty(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=0)
        book.apply_delta([["65000.0", "5.0"]], [], seq=1)
        bb = book.best_bid()
        assert bb[1] == pytest.approx(5.0)

    def test_delta_removes_level_when_qty_zero(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=0)
        book.apply_delta([["65000.0", "0"]], [], seq=1)
        bb = book.best_bid()
        # Best bid should now be 64999
        assert bb[0] == pytest.approx(64999.0)

    def test_gap_detection(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=0)
        result = book.apply_delta([], [], seq=5)  # Gap: expected 1, got 5
        assert result is False
        assert book._gap_count == 1

    def test_consecutive_seq_accepted(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=0)
        result = book.apply_delta([], [], seq=1)
        assert result is True

    def test_delta_rejected_if_not_initialized(self, book):
        result = book.apply_delta(_snap_bids(), _snap_asks())
        assert result is False

    def test_seq_minus_one_skips_gap_check(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=0)
        result = book.apply_delta([], [], seq=-1)
        assert result is True


class TestSnapshot2:
    def test_snapshot_output_format(self, book):
        book.apply_snapshot(_snap_bids(), _snap_asks(), seq=42)
        snap = book.snapshot(levels=3)
        assert len(snap["bids"]) == 3
        assert len(snap["asks"]) == 3
        assert snap["seq"] == 42
        # Bids should be descending
        assert snap["bids"][0][0] > snap["bids"][1][0]
        # Asks should be ascending
        assert snap["asks"][0][0] < snap["asks"][1][0]
