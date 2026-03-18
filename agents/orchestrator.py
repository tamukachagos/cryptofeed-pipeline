"""Agent Orchestrator — runs all agents on their schedules.

Like a cron supervisor. Run this on the VM and it manages all agents.

Usage:
    python agents/orchestrator.py

Schedule:
    Every 5 min  : exchange_health, vercel_watchdog
    Every 1 hour : quality_monitor, vercel_performance
    Every 6 hours: gap_filler, vercel_env_sync
    Daily 00:05  : daily_report, vercel_daily_report
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from loguru import logger

SCHEDULE = [
    {"name": "exchange_health",       "module": "agents.exchange_health",  "fn": "check_health",       "interval_s": 300},
    {"name": "vercel_watchdog",       "module": "agents.vercel_agent",     "fn": "check_deployments",  "interval_s": 300},
    {"name": "quality_monitor",       "module": "agents.quality_monitor",  "fn": "main",               "interval_s": 3600},
    {"name": "vercel_performance",    "module": "agents.vercel_agent",     "fn": "audit_performance",  "interval_s": 3600},
    {"name": "gap_filler",            "module": "agents.gap_filler",       "fn": "main",               "interval_s": 21600},
    {"name": "vercel_env_sync",       "module": "agents.vercel_agent",     "fn": "sync_environment",   "interval_s": 21600},
    {"name": "daily_report",          "module": "agents.daily_report",     "fn": "main",               "interval_s": 86400},
    {"name": "vercel_daily_report",   "module": "agents.vercel_agent",     "fn": "daily_report",       "interval_s": 86400},
]

_last_run: dict[str, float] = {}


def should_run(name: str, interval_s: int) -> bool:
    now = time.monotonic()
    last = _last_run.get(name, 0)
    return (now - last) >= interval_s


def run_agent(config: dict) -> None:
    import importlib
    try:
        mod = importlib.import_module(config["module"])
        fn = getattr(mod, config["fn"])
        logger.info(f"[orchestrator] Running {config['name']}...")
        fn()
        logger.success(f"[orchestrator] {config['name']} completed.")
    except Exception as exc:
        logger.error(f"[orchestrator] {config['name']} failed: {exc}")
    _last_run[config["name"]] = time.monotonic()


def main():
    logger.info("Agent Orchestrator started.")
    logger.info(f"Agents: {[s['name'] for s in SCHEDULE]}")

    # Run all agents immediately on startup
    for config in SCHEDULE:
        run_agent(config)

    while True:
        time.sleep(30)  # Check every 30s
        for config in SCHEDULE:
            if should_run(config["name"], config["interval_s"]):
                run_agent(config)


if __name__ == "__main__":
    main()
