"""Unit tests for microstructure features."""
import numpy as np
import pandas as pd
import pytest

from features.microstructure import (
    compute_kyle_lambda,
    compute_obi,
    compute_trade_imbalance,
    compute_vpin,
)


def _make_trade_df(sides: list[str], prices=None, qtys=None) -> pd.DataFrame:
    n = len(sides)
    if prices is None:
        prices = [100.0 + i * 0.1 for i in range(n)]
    if qtys is None:
        qtys = [1.0] * n
    return pd.DataFrame({
        "exchange": "test",
        "symbol": "BTCUSDT",
        "timestamp_ns": list(range(n)),
        "trade_id": [str(i) for i in range(n)],
        "price": prices,
        "qty": qtys,
        "side": sides,
        "is_liquidation": False,
    })


def _make_book_df(obi_vals: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp_ns": list(range(len(obi_vals))),
        "obi_5": obi_vals,
    })


class TestOBI:
    def test_from_column(self):
        df = _make_book_df([0.3, -0.2, 0.1])
        result = compute_obi(df, levels=5)
        assert list(result) == pytest.approx([0.3, -0.2, 0.1])

    def test_returns_values_in_range(self):
        df = _make_book_df([0.8, -0.9, 0.0])
        result = compute_obi(df)
        assert all(-1 <= v <= 1 for v in result)


class TestTradeImbalance:
    def test_all_buys_returns_one(self):
        df = _make_trade_df(["buy"] * 200)
        result = compute_trade_imbalance(df, window=100)
        # All buys → imbalance should be 1.0
        assert result.iloc[-1] == pytest.approx(1.0)

    def test_all_sells_returns_zero(self):
        df = _make_trade_df(["sell"] * 200)
        result = compute_trade_imbalance(df, window=100)
        # All sells → imbalance should be 0.0
        assert result.iloc[-1] == pytest.approx(0.0)

    def test_balanced_returns_half(self):
        sides = ["buy", "sell"] * 100
        df = _make_trade_df(sides)
        result = compute_trade_imbalance(df, window=100)
        assert result.iloc[-1] == pytest.approx(0.5, abs=0.05)

    def test_empty_df(self):
        result = compute_trade_imbalance(pd.DataFrame(), window=100)
        assert result.empty

    def test_result_in_range(self):
        sides = np.random.choice(["buy", "sell"], 300).tolist()
        df = _make_trade_df(sides)
        result = compute_trade_imbalance(df, window=50)
        assert result.dropna().between(0, 1).all()


class TestVPIN:
    def test_all_buys_high_vpin(self):
        df = _make_trade_df(["buy"] * 1000, qtys=[1.0] * 1000)
        result = compute_vpin(df, bucket_size=0.1)
        assert len(result) > 0
        assert result.mean() == pytest.approx(1.0, abs=0.01)

    def test_balanced_low_vpin(self):
        sides = ["buy", "sell"] * 500
        df = _make_trade_df(sides, qtys=[1.0] * 1000)
        result = compute_vpin(df, bucket_size=0.1)
        assert len(result) > 0
        assert result.mean() < 0.2  # Should be near 0 for balanced flow

    def test_empty_df(self):
        result = compute_vpin(pd.DataFrame(columns=["qty", "side"]), bucket_size=0.1)
        assert result.empty


class TestKyleLambda:
    def test_positive_lambda_when_buys_push_price_up(self):
        # price[i] - price[i-1] is driven by sides[i] so λ > 0
        n = 200
        sides = ["buy" if i % 2 == 0 else "sell" for i in range(n)]
        prices = [100.0]
        for i in range(1, n):
            delta = 0.5 if sides[i] == "buy" else -0.5
            prices.append(prices[-1] + delta)

        df = _make_trade_df(sides, prices=prices)
        lam = compute_kyle_lambda(df)
        assert not np.isnan(lam)
        assert lam > 0  # Buying pushes price up

    def test_insufficient_data_returns_nan(self):
        df = _make_trade_df(["buy"] * 5)
        lam = compute_kyle_lambda(df)
        assert np.isnan(lam)
