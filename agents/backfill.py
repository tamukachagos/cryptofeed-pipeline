"""Historical data backfill — downloads 90 days from public exchange archives.

Sources:
  Binance  — data.binance.vision (aggTrades + fundingRate) — free, bulk ZIP
  Bybit    — public.bybit.com   (trades)                   — free, bulk CSV.GZ
  OKX      — REST API           (trades + funding)          — free, paginated

Usage:
    python agents/backfill.py --days 90
    python agents/backfill.py --days 90 --exchange binance
    python agents/backfill.py --days 7 --symbol BTCUSDT
    python agents/backfill.py --resume          # skip already-downloaded dates
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import pandas as pd
import requests
from loguru import logger

DATA_DIR   = Path(os.getenv("DATA_DIR", "./data"))
SYMBOLS    = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",")
COMPRESS   = os.getenv("PARQUET_COMPRESSION", "zstd")
DONE_FILE  = Path("logs/backfill_done.json")
DONE_FILE.parent.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CryptoDataAPI-Backfill/1.0"})


# ─── Progress tracker ────────────────────────────────────────────────────────

def _load_done() -> set:
    if DONE_FILE.exists():
        return set(json.loads(DONE_FILE.read_text()))
    return set()

def _mark_done(key: str) -> None:
    done = _load_done()
    done.add(key)
    DONE_FILE.write_text(json.dumps(sorted(done)))


# ─── Parquet writer (simple, no flush logic needed for backfill) ─────────────

def _write_parquet(df: pd.DataFrame, data_type: str, exchange: str, symbol: str, date_str: str) -> None:
    if df.empty:
        return
    hour_col = "hour"
    if "timestamp_ns" in df.columns:
        df[hour_col] = pd.to_datetime(df["timestamp_ns"], unit="ns", utc=True).dt.hour
    else:
        df[hour_col] = 0

    for hour, group in df.groupby(hour_col):
        out = DATA_DIR / data_type / exchange / symbol / date_str / f"{int(hour):02d}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        group.drop(columns=[hour_col]).to_parquet(out, compression=COMPRESS, index=False)

    total = len(df)
    logger.success(f"[backfill] Wrote {total:,} rows → {data_type}/{exchange}/{symbol}/{date_str}")


# ─── BINANCE VISION ──────────────────────────────────────────────────────────

BINANCE_BASE = "https://data.binance.vision/data/futures/um/daily"

def _binance_url(data_type: str, symbol: str, date_str: str) -> str:
    fname = f"{symbol}-{data_type}-{date_str}.zip"
    return f"{BINANCE_BASE}/{data_type}/{symbol}/{fname}"


def backfill_binance_trades(symbol: str, date_str: str, resume: bool) -> bool:
    key = f"binance:trades:{symbol}:{date_str}"
    if resume and key in _load_done():
        logger.info(f"[backfill] Skip (done): {key}")
        return True

    url = _binance_url("aggTrades", symbol, date_str)
    try:
        r = SESSION.get(url, timeout=60)
        if r.status_code == 404:
            logger.warning(f"[backfill] Not available: binance aggTrades {symbol} {date_str}")
            return False
        r.raise_for_status()
    except Exception as e:
        logger.error(f"[backfill] Binance download failed {symbol} {date_str}: {e}")
        return False

    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, header=None, names=[
                    "agg_trade_id", "price", "qty", "first_trade_id", "last_trade_id",
                    "transact_time", "is_buyer_maker"
                ])

        df["exchange"]       = "binance"
        df["symbol"]         = symbol
        df["timestamp_ns"]   = (df["transact_time"].astype("int64") * 1_000_000)
        df["trade_id"]       = df["last_trade_id"].astype(str)
        df["price"]          = df["price"].astype(float)
        df["qty"]            = df["qty"].astype(float)
        df["side"]           = df["is_buyer_maker"].apply(lambda m: "sell" if m else "buy")
        df["is_liquidation"] = False

        result = df[["exchange", "symbol", "timestamp_ns", "trade_id", "price", "qty", "side", "is_liquidation"]]
        _write_parquet(result, "trades", "binance", symbol, date_str)
        _mark_done(key)
        return True

    except Exception as e:
        logger.error(f"[backfill] Binance parse failed {symbol} {date_str}: {e}")
        return False


def backfill_binance_funding(symbol: str, date_str: str, resume: bool) -> bool:
    key = f"binance:funding:{symbol}:{date_str}"
    if resume and key in _load_done():
        return True

    url = _binance_url("fundingRate", symbol, date_str)
    try:
        r = SESSION.get(url, timeout=60)
        if r.status_code == 404:
            return False
        r.raise_for_status()
    except Exception as e:
        logger.error(f"[backfill] Binance funding download failed: {e}")
        return False

    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(f, header=None, names=[
                    "calc_time", "funding_interval_hours", "last_funding_rate"
                ])

        df["exchange"]     = "binance"
        df["symbol"]       = symbol
        df["timestamp_ns"] = (df["calc_time"].astype("int64") * 1_000_000)
        df["funding_rate"] = df["last_funding_rate"].astype(float)
        df["mark_price"]   = 0.0

        result = df[["exchange", "symbol", "timestamp_ns", "funding_rate", "mark_price"]]
        _write_parquet(result, "funding", "binance", symbol, date_str)
        _mark_done(key)
        return True

    except Exception as e:
        logger.error(f"[backfill] Binance funding parse failed: {e}")
        return False


# ─── BYBIT PUBLIC DATA ───────────────────────────────────────────────────────

BYBIT_BASE = "https://public.bybit.com/trading"

def backfill_bybit_trades(symbol: str, date_str: str, resume: bool) -> bool:
    key = f"bybit:trades:{symbol}:{date_str}"
    if resume and key in _load_done():
        return True

    url = f"{BYBIT_BASE}/{symbol}/{symbol}{date_str}.csv.gz"
    try:
        r = SESSION.get(url, timeout=120)
        if r.status_code == 404:
            logger.warning(f"[backfill] Not available: bybit trades {symbol} {date_str}")
            return False
        r.raise_for_status()
    except Exception as e:
        logger.error(f"[backfill] Bybit download failed {symbol} {date_str}: {e}")
        return False

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
            df = pd.read_csv(gz)

        # Bybit columns: timestamp, symbol, side, size, price, tickDirection, trdMatchID, ...
        df["exchange"]       = "bybit"
        df["symbol"]         = symbol
        df["timestamp_ns"]   = (df["timestamp"].astype(float) * 1_000_000_000).astype("int64")
        df["trade_id"]       = df["trdMatchID"].astype(str) if "trdMatchID" in df.columns else df.index.astype(str)
        df["price"]          = df["price"].astype(float)
        df["qty"]            = df["size"].astype(float)
        df["side"]           = df["side"].str.lower()
        df["is_liquidation"] = False

        result = df[["exchange", "symbol", "timestamp_ns", "trade_id", "price", "qty", "side", "is_liquidation"]]
        _write_parquet(result, "trades", "bybit", symbol, date_str)
        _mark_done(key)
        return True

    except Exception as e:
        logger.error(f"[backfill] Bybit parse failed {symbol} {date_str}: {e}")
        return False


# ─── OKX REST API ────────────────────────────────────────────────────────────

OKX_BASE = "https://www.okx.com"

def _okx_symbol(symbol: str) -> str:
    """Convert BTCUSDT → BTC-USDT-SWAP"""
    base = symbol.replace("USDT", "")
    return f"{base}-USDT-SWAP"

def backfill_okx_trades(symbol: str, date_str: str, resume: bool) -> bool:
    key = f"okx:trades:{symbol}:{date_str}"
    if resume and key in _load_done():
        return True

    inst_id = _okx_symbol(symbol)
    date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(date.timestamp() * 1000)
    end_ms   = int((date + timedelta(days=1)).timestamp() * 1000)

    all_rows = []
    after = None

    for _ in range(200):  # max 200 pages × 100 = 20,000 trades
        params = {"instId": inst_id, "limit": 100}
        if after:
            params["after"] = after

        try:
            r = SESSION.get(f"{OKX_BASE}/api/v5/market/history-trades", params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            trades = data.get("data", [])
        except Exception as e:
            logger.error(f"[backfill] OKX API failed: {e}")
            break

        if not trades:
            break

        for t in trades:
            ts_ms = int(t["ts"])
            if ts_ms < start_ms:
                all_rows = [r for r in all_rows if int(r["ts"]) >= start_ms]
                goto_next = False
                break
            if ts_ms <= end_ms:
                all_rows.append(t)
        else:
            after = trades[-1]["tradeId"]
            time.sleep(0.1)
            continue
        break

    if not all_rows:
        logger.warning(f"[backfill] OKX: no trades for {symbol} {date_str}")
        return False

    df = pd.DataFrame(all_rows)
    df["exchange"]       = "okx"
    df["symbol"]         = symbol
    df["timestamp_ns"]   = (df["ts"].astype("int64") * 1_000_000)
    df["trade_id"]       = df["tradeId"].astype(str)
    df["price"]          = df["px"].astype(float)
    df["qty"]            = df["sz"].astype(float)
    df["side"]           = df["side"].str.lower()
    df["is_liquidation"] = False

    result = df[["exchange", "symbol", "timestamp_ns", "trade_id", "price", "qty", "side", "is_liquidation"]]
    _write_parquet(result, "trades", "okx", symbol, date_str)
    _mark_done(key)
    return True


def backfill_okx_funding(symbol: str, date_str: str, resume: bool) -> bool:
    key = f"okx:funding:{symbol}:{date_str}"
    if resume and key in _load_done():
        return True

    inst_id = _okx_symbol(symbol)
    date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(date.timestamp() * 1000)
    end_ms   = int((date + timedelta(days=1)).timestamp() * 1000)

    all_rows = []
    try:
        r = SESSION.get(
            f"{OKX_BASE}/api/v5/public/funding-rate-history",
            params={"instId": inst_id, "before": start_ms, "after": end_ms, "limit": 100},
            timeout=15,
        )
        r.raise_for_status()
        all_rows = r.json().get("data", [])
    except Exception as e:
        logger.error(f"[backfill] OKX funding failed: {e}")
        return False

    if not all_rows:
        return False

    df = pd.DataFrame(all_rows)
    df["exchange"]     = "okx"
    df["symbol"]       = symbol
    df["timestamp_ns"] = (df["fundingTime"].astype("int64") * 1_000_000)
    df["funding_rate"] = df["fundingRate"].astype(float)
    df["mark_price"]   = 0.0

    result = df[["exchange", "symbol", "timestamp_ns", "funding_rate", "mark_price"]]
    _write_parquet(result, "funding", "okx", symbol, date_str)
    _mark_done(key)
    return True


# ─── Main ────────────────────────────────────────────────────────────────────

def run_backfill(days: int = 90, exchanges: list[str] | None = None,
                 symbols: list[str] | None = None, resume: bool = True) -> None:

    if exchanges is None:
        exchanges = ["binance", "bybit", "okx"]
    if symbols is None:
        symbols = SYMBOLS

    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, days + 1)]

    logger.info(f"[backfill] Starting: {days} days, {len(exchanges)} exchanges, {len(symbols)} symbols")
    logger.info(f"[backfill] Date range: {dates[-1]} → {dates[0]}")

    total = ok = skipped = failed = 0

    for date_str in reversed(dates):  # oldest first
        for symbol in symbols:
            for exchange in exchanges:
                total += 1

                if exchange == "binance":
                    r1 = backfill_binance_trades(symbol, date_str, resume)
                    r2 = backfill_binance_funding(symbol, date_str, resume)
                    success = r1
                elif exchange == "bybit":
                    success = backfill_bybit_trades(symbol, date_str, resume)
                elif exchange == "okx":
                    r1 = backfill_okx_trades(symbol, date_str, resume)
                    r2 = backfill_okx_funding(symbol, date_str, resume)
                    success = r1
                else:
                    continue

                if success:
                    ok += 1
                else:
                    failed += 1

                # Polite delay
                time.sleep(0.2)

        # Print progress every day
        logger.info(f"[backfill] {date_str} done — total so far: {ok} ok / {failed} failed")

    logger.success(f"\n[backfill] Complete: {ok}/{total} successful, {failed} failed")
    size = sum(f.stat().st_size for f in DATA_DIR.rglob("*.parquet"))
    logger.success(f"[backfill] Total data size: {size / 1e9:.2f} GB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill 90 days of historical crypto data")
    parser.add_argument("--days",     type=int, default=90,  help="Number of days to backfill")
    parser.add_argument("--exchange", type=str, default=None, help="binance, bybit, or okx (default: all)")
    parser.add_argument("--symbol",   type=str, default=None, help="e.g. BTCUSDT (default: all)")
    parser.add_argument("--resume",   action="store_true", default=True, help="Skip already-downloaded dates")
    parser.add_argument("--no-resume", action="store_true", help="Re-download everything")
    args = parser.parse_args()

    exchanges = [args.exchange] if args.exchange else None
    symbols   = [args.symbol]   if args.symbol   else None
    resume    = not args.no_resume

    run_backfill(days=args.days, exchanges=exchanges, symbols=symbols, resume=resume)
