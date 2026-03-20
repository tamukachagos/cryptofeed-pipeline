"""
Layer 3 + 4 API — Intelligence and Feature Store endpoints.

GET  /v1/features              Precomputed microstructure feature bundle
GET  /v1/regime                Current market regime classification
GET  /v1/anomalies             Recent anomaly events (last N minutes)
GET  /v1/arb                   Active arbitrage signals
POST /v1/triggers              Register a condition-based trigger
GET  /v1/triggers              List registered triggers
DELETE /v1/triggers/{id}       Remove a trigger
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.auth import optional_auth, APIKeyInfo
from api.rate_limiter import check_rate_limit
from intelligence.regime_detector import RegimeDetector
from intelligence.anomaly_detector import AnomalyDetector, ExchangeTrustScorer
from intelligence.arb_detector import ArbDetector
from intelligence.trigger_engine import TriggerEngine
from features.feature_store import FeatureStore

router = APIRouter(tags=["intelligence"])

# Module-level singletons
_regime_detector  = RegimeDetector()
_anomaly_detector = AnomalyDetector()
_arb_detector     = ArbDetector()
_trigger_engine   = TriggerEngine()
_trust_scorer     = ExchangeTrustScorer()
_feature_store:   Optional[FeatureStore] = None
_replay_engine = None


def _get_feature_store() -> FeatureStore:
    global _feature_store
    if _feature_store is None:
        _feature_store = FeatureStore(data_dir=os.getenv("DATA_DIR", "./data"))
    return _feature_store


def _get_replay_engine():
    global _replay_engine
    if _replay_engine is None:
        import sys
        from pathlib import Path
        root = str(Path(__file__).resolve().parents[2])
        if root not in sys.path:
            sys.path.insert(0, root)
        from replay.engine import ReplayEngine
        _replay_engine = ReplayEngine(data_dir=os.getenv("DATA_DIR", "./data"))
    return _replay_engine


def _load_data(exchange: str, symbol: str, minutes: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load trade, book, liquidation data for the given window."""
    engine = _get_replay_engine()
    end   = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    trade_df = engine._load("trades",       exchange, symbol, start, end)
    book_df  = engine._load("books",        exchange, symbol, start, end)
    liq_df   = engine._load("liquidations", exchange, symbol, start, end)
    return trade_df, book_df, liq_df


_VALID_EXCHANGES = {"binance", "bybit", "okx"}


def _validate(exchange: str, symbol: str) -> None:
    import re
    if exchange.lower() not in _VALID_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"Unknown exchange: {exchange}")
    if not re.match(r"^[A-Z0-9]{3,20}$", symbol.upper()):
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {symbol}")


# ── Feature Store ─────────────────────────────────────────────────────────────

@router.get("/v1/features")
async def get_features(
    exchange: str = Query(..., description="Exchange: binance, bybit, okx"),
    symbol:   str = Query(..., description="Symbol e.g. BTCUSDT"),
    window:   int = Query(60, ge=5, le=1440, description="Lookback window in minutes"),
    key: APIKeyInfo = Depends(optional_auth),
):
    """
    Return precomputed microstructure feature bundle.

    Includes: OBI, VPIN, Kyle's λ, trade imbalance, realised vol, momentum,
    VWAP, spread bps, vol regime ratio.
    """
    await check_rate_limit(key)
    _validate(exchange, symbol)
    return _get_feature_store().compute(exchange.lower(), symbol.upper(), window)


# ── Regime Detection ──────────────────────────────────────────────────────────

@router.get("/v1/regime")
async def get_regime(
    exchange: str = Query(...),
    symbol:   str = Query(...),
    window:   int = Query(300, ge=50, le=5000, description="Number of recent trades to analyse"),
    key: APIKeyInfo = Depends(optional_auth),
):
    """
    Classify the current market regime for an exchange/symbol.

    Returns: regime label, confidence, momentum, vol, flow imbalance,
             structural break flag, and contributing signals.
    """
    await check_rate_limit(key)
    _validate(exchange, symbol)
    trade_df, book_df, _ = _load_data(exchange.lower(), symbol.upper(), minutes=60)

    if trade_df.empty:
        raise HTTPException(status_code=404, detail="No trade data available for this exchange/symbol")

    snap = _regime_detector.detect(
        exchange.lower(), symbol.upper(), trade_df, book_df, window=window
    )
    return snap.to_dict()


# ── Anomaly Detection ─────────────────────────────────────────────────────────

@router.get("/v1/anomalies")
async def get_anomalies(
    exchange:    str = Query(...),
    symbol:      str = Query(...),
    window_min:  int = Query(15, ge=1, le=120, description="Lookback window minutes"),
    min_severity: float = Query(0.3, ge=0.0, le=1.0),
    key: APIKeyInfo = Depends(optional_auth),
):
    """
    Return detected anomaly events over the last `window_min` minutes.

    Detects: wash trading, liquidation cascades, momentum ignition,
             quote stuffing, price outliers.

    Includes exchange trust score showing data reliability.
    """
    await check_rate_limit(key)
    _validate(exchange, symbol)
    trade_df, book_df, liq_df = _load_data(exchange.lower(), symbol.upper(), minutes=window_min)

    events = _anomaly_detector.run_all(
        exchange.lower(), symbol.upper(), trade_df, book_df, liq_df
    )

    filtered = [e for e in events if e.severity >= min_severity]

    # Update trust score
    for e in filtered:
        _trust_scorer.penalise(exchange.lower(), e.severity)

    return {
        "exchange":    exchange,
        "symbol":      symbol,
        "window_min":  window_min,
        "trust_score": round(_trust_scorer.score(exchange.lower()), 4),
        "anomaly_count": len(filtered),
        "anomalies":   [e.to_dict() for e in filtered],
    }


# ── Arbitrage Signals ─────────────────────────────────────────────────────────

@router.get("/v1/arb")
async def get_arb_signals(
    symbol:      str   = Query(...),
    min_edge_bps: float = Query(3.0, ge=0.0),
    key: APIKeyInfo = Depends(optional_auth),
):
    """
    Return active cross-venue and funding arbitrage signals.

    Compares Binance, Bybit, and OKX midprices and funding rates.
    Returns signals where net edge (after fees) exceeds min_edge_bps.
    """
    await check_rate_limit(key)
    sym = symbol.upper()
    import re
    if not re.match(r"^[A-Z0-9]{3,20}$", sym):
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {symbol}")

    # Gather latest prices and funding from each exchange
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=5)

    prices: dict[str, float]       = {}
    spreads: dict[str, float]      = {}
    funding_rates: dict[str, float] = {}
    mark_prices: dict[str, float]   = {}

    engine = _get_replay_engine()

    for ex in ["binance", "bybit", "okx"]:
        trade_df = engine._load("trades", ex, sym, start, now)
        if not trade_df.empty:
            prices[ex] = float(trade_df["price"].iloc[-1])

        book_df = engine._load("books", ex, sym, start, now)
        if not book_df.empty and "spread" in book_df.columns and "midprice" in book_df.columns:
            mid  = book_df["midprice"].replace("", pd.NA).astype(float).dropna()
            sprd = book_df["spread"].replace("", pd.NA).astype(float).dropna()
            if not mid.empty and mid.iloc[-1] > 0:
                spreads[ex] = float(sprd.iloc[-1] / mid.iloc[-1] * 10_000)

        funding_df = engine._load("funding", ex, sym, start, now)
        if not funding_df.empty and "funding_rate" in funding_df.columns:
            funding_rates[ex] = float(funding_df["funding_rate"].iloc[-1])

        mark_df = engine._load("mark_price", ex, sym, start, now)
        if not mark_df.empty and "mark_price" in mark_df.columns:
            mark_prices[ex] = float(mark_df["mark_price"].replace("", pd.NA).astype(float).dropna().iloc[-1])

    ts_ns = int(now.timestamp() * 1e9)
    signals = []

    if len(prices) >= 2:
        signals += _arb_detector.detect_cross_venue(sym, prices, spreads, ts_ns)

    if funding_rates:
        signals += _arb_detector.detect_funding_carry(sym, funding_rates, prices, ts_ns)

    if mark_prices and prices:
        signals += _arb_detector.detect_mark_spot_divergence(sym, mark_prices, prices, ts_ns)

    filtered = [s for s in signals if s.edge_bps >= min_edge_bps]

    return {
        "symbol":        sym,
        "signal_count":  len(filtered),
        "prices_by_exchange":   prices,
        "funding_by_exchange":  funding_rates,
        "signals":       [s.to_dict() for s in filtered],
    }


# ── Trigger Engine ────────────────────────────────────────────────────────────

class TriggerDefRequest(BaseModel):
    id:          str
    symbol:      str = "*"
    conditions:  list[dict]
    require_all: bool = True
    cooldown_s:  float = 60.0


@router.post("/v1/triggers", status_code=201)
async def create_trigger(
    body: TriggerDefRequest,
    key: APIKeyInfo = Depends(optional_auth),
):
    """Register a condition-based trigger. Fires on regime, anomaly, or arb events."""
    await check_rate_limit(key)
    if key.plan == "free":
        raise HTTPException(status_code=403, detail="Triggers require Pro plan or above")
    # Namespace trigger ID by key_hash to prevent cross-user collisions
    owned_id = f"{key.key_hash[:16]}:{body.id}"
    data = body.model_dump()
    data["id"] = owned_id
    data["_owner"] = key.key_hash
    _trigger_engine.register(data)
    return {"status": "registered", "trigger_id": body.id}


@router.get("/v1/triggers")
async def list_triggers(key: APIKeyInfo = Depends(optional_auth)):
    """List triggers owned by the authenticated key."""
    await check_rate_limit(key)
    all_triggers = _trigger_engine.list_triggers()
    # Return only triggers owned by this key
    owned = [
        {**t, "id": t["id"].split(":", 1)[-1]}   # strip key_hash prefix for display
        for t in all_triggers
        if t.get("_owner") == key.key_hash
    ]
    return {"triggers": owned}


@router.delete("/v1/triggers/{trigger_id}")
async def delete_trigger(trigger_id: str, key: APIKeyInfo = Depends(optional_auth)):
    """Remove a trigger owned by the authenticated key."""
    await check_rate_limit(key)
    owned_id = f"{key.key_hash[:16]}:{trigger_id}"
    # Verify ownership before deletion
    all_triggers = _trigger_engine.list_triggers()
    match = next((t for t in all_triggers if t["id"] == owned_id and t.get("_owner") == key.key_hash), None)
    if match is None:
        raise HTTPException(status_code=404, detail="Trigger not found")
    _trigger_engine.unregister(owned_id)
    return {"status": "removed", "trigger_id": trigger_id}
