"""Backfill Watchdog Agent.

Monitors the primary backfill job by watching logs/backfill_done.json.
When the file stops being updated (idle for 15+ minutes), assumes the
backfill is complete and automatically runs correction passes:

  1. Binance trades  — re-runs with fixed CSV header parsing
  2. OKX OHLCV       — re-runs with fixed pagination (after=end_ms)

Uses logs/backfill_corrections_done.json as a sentinel to only run once.
Runs every 5 minutes via orchestrator — is a no-op once corrections are done.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

DONE_FILE         = Path("logs/backfill_done.json")
CORRECTIONS_STATE = Path("logs/backfill_corrections_done.json")
IDLE_THRESHOLD_MIN = 15   # Minutes without new entries before we declare primary backfill done
MIN_ENTRIES        = 10   # Require at least this many done entries before checking idle


def _load_state() -> dict:
    if CORRECTIONS_STATE.exists():
        return json.loads(CORRECTIONS_STATE.read_text())
    return {}


def _save_state(state: dict) -> None:
    CORRECTIONS_STATE.parent.mkdir(parents=True, exist_ok=True)
    CORRECTIONS_STATE.write_text(json.dumps(state, indent=2))


def main() -> None:
    state = _load_state()

    if state.get("all_done"):
        return  # Nothing left to do

    if not DONE_FILE.exists():
        logger.debug("[backfill_watchdog] No backfill_done.json yet — primary job hasn't started.")
        return

    # Check how long ago the done file was last updated
    mtime = DONE_FILE.stat().st_mtime
    idle_minutes = (time.time() - mtime) / 60

    done_keys: set = set(json.loads(DONE_FILE.read_text()))
    entry_count = len(done_keys)

    logger.info(
        f"[backfill_watchdog] {entry_count} entries in done file, "
        f"idle for {idle_minutes:.1f} min (threshold: {IDLE_THRESHOLD_MIN} min)"
    )

    if entry_count < MIN_ENTRIES or idle_minutes < IDLE_THRESHOLD_MIN:
        logger.info("[backfill_watchdog] Primary backfill still running — waiting.")
        return

    logger.info("[backfill_watchdog] Primary backfill appears complete. Starting corrections…")

    from agents.backfill import run_backfill

    # ── Correction 1: Binance trades (CSV header fix) ──────────────────────
    if not state.get("binance_trades_done"):
        logger.info("[backfill_watchdog] Running Binance trades correction (90 days, no-resume)…")
        try:
            run_backfill(days=90, exchanges=["binance"], data_types=["trades"], resume=False)
            state["binance_trades_done"] = datetime.now(timezone.utc).isoformat()
            _save_state(state)
            logger.success("[backfill_watchdog] Binance trades correction complete.")
        except Exception as e:
            logger.error(f"[backfill_watchdog] Binance trades correction failed: {e}")
            return  # Retry next run

    # ── Correction 2: OKX OHLCV (pagination fix) ───────────────────────────
    if not state.get("okx_ohlcv_done"):
        logger.info("[backfill_watchdog] Running OKX OHLCV correction (90 days, no-resume)…")
        try:
            run_backfill(days=90, exchanges=["okx"], data_types=["ohlcv"], resume=False)
            state["okx_ohlcv_done"] = datetime.now(timezone.utc).isoformat()
            _save_state(state)
            logger.success("[backfill_watchdog] OKX OHLCV correction complete.")
        except Exception as e:
            logger.error(f"[backfill_watchdog] OKX OHLCV correction failed: {e}")
            return  # Retry next run

    # ── All corrections done ───────────────────────────────────────────────
    state["all_done"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)
    logger.success("[backfill_watchdog] All corrections complete. Watchdog retiring.")
