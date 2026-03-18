"""Unit tests for message normalization."""
import json
import pytest
from processor.normalizer import (
    normalize_binance_trade,
    normalize_binance_funding,
    normalize_bybit_trade,
    normalize_okx_trade,
    normalize_okx_funding,
    normalize_symbol,
)


class TestSymbolNormalization:
    def test_btcusdt_unchanged(self):
        assert normalize_symbol("BTCUSDT") == "BTCUSDT"

    def test_okx_swap_format(self):
        assert normalize_symbol("BTC-USDT-SWAP") == "BTCUSDT"

    def test_gateio_format(self):
        assert normalize_symbol("BTC_USDT") == "BTCUSDT"

    def test_eth(self):
        assert normalize_symbol("ETH-USDT-SWAP") == "ETHUSDT"

    def test_xrp(self):
        assert normalize_symbol("XRP_USDT") == "XRPUSDT"


class TestBinanceTrade:
    def _raw(self, **overrides):
        base = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "trade_id": "abc123",
            "price": 65000.5,
            "qty": 0.01,
            "side": "buy",
            "is_liquidation": False,
        }
        base.update(overrides)
        return base

    def test_basic_normalization(self):
        raw = self._raw()
        trade = normalize_binance_trade(raw)
        assert trade.exchange == "binance"
        assert trade.symbol == "BTCUSDT"
        assert trade.price == pytest.approx(65000.5)
        assert trade.qty == pytest.approx(0.01)
        assert trade.side == "buy"
        assert trade.is_liquidation is False

    def test_sell_side(self):
        raw = self._raw(side="sell")
        trade = normalize_binance_trade(raw)
        assert trade.side == "sell"

    def test_liquidation_flag(self):
        raw = self._raw(is_liquidation=True)
        trade = normalize_binance_trade(raw)
        assert trade.is_liquidation is True

    def test_string_is_liquidation(self):
        raw = self._raw(is_liquidation="True")
        trade = normalize_binance_trade(raw)
        assert trade.is_liquidation is True


class TestBinanceFunding:
    def test_basic(self):
        raw = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "funding_rate": 0.0001,
            "next_funding_time_ns": 1_700_028_800_000_000_000,
        }
        funding = normalize_binance_funding(raw)
        assert funding.exchange == "binance"
        assert funding.funding_rate == pytest.approx(0.0001)
        assert funding.next_funding_time_ns == 1_700_028_800_000_000_000

    def test_empty_next_funding(self):
        raw = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "funding_rate": 0.0002,
            "next_funding_time_ns": "",
        }
        funding = normalize_binance_funding(raw)
        assert funding.next_funding_time_ns is None


class TestBybitTrade:
    def test_basic(self):
        raw = {
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "trade_id": "xyz",
            "price": 64999.0,
            "qty": 0.05,
            "side": "sell",
            "is_liquidation": False,
        }
        trade = normalize_bybit_trade(raw)
        assert trade.exchange == "bybit"
        assert trade.side == "sell"
        assert trade.price == pytest.approx(64999.0)


class TestOKXTrade:
    def test_swap_symbol(self):
        raw = {
            "exchange": "okx",
            "symbol": "BTC-USDT-SWAP",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "trade_id": "okx_1",
            "price": 65100.0,
            "qty": 0.1,
            "side": "buy",
            "is_liquidation": False,
        }
        trade = normalize_okx_trade(raw)
        assert trade.symbol == "BTCUSDT"
        assert trade.exchange == "okx"


class TestOKXFunding:
    def test_basic(self):
        raw = {
            "exchange": "okx",
            "symbol": "BTC-USDT-SWAP",
            "timestamp_ns": 1_700_000_000_000_000_000,
            "funding_rate": -0.0003,
            "next_funding_time_ns": 1_700_028_800_000_000_000,
        }
        funding = normalize_okx_funding(raw)
        assert funding.symbol == "BTCUSDT"
        assert funding.funding_rate == pytest.approx(-0.0003)
