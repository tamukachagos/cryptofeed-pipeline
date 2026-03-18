"""Exchange Health Monitor — checks Redis streams every 5 min, restarts collector if stale.

Usage:
    python agents/exchange_health.py          # run once
    python agents/exchange_health.py --loop   # run every 5 minutes
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import redis
from loguru import logger

HEALTH_LOG = Path("logs/exchange_health.jsonl")
CHECK_INTERVAL_S = 300   # 5 minutes
STALE_THRESHOLD_S = 300  # stream is "stale" if no new messages in 5 min

STREAMS_TO_WATCH = [
    "binance:trades:BTCUSDT",
    "bybit:trades:BTCUSDT",
    "okx:trades:BTCUSDT",
    "binance:depth:BTCUSDT",
]


def get_stream_stats(r: redis.Redis) -> dict[str, dict]:
    stats = {}
    for stream in STREAMS_TO_WATCH:
        try:
            length = r.xlen(stream)
            # Get the timestamp of the last message
            last = r.xrevrange(stream, count=1)
            if last:
                ts_ns = int(last[0][1].get("timestamp_ns", 0))
                age_s = (time.time() * 1e9 - ts_ns) / 1e9
            else:
                age_s = float("inf")
            stats[stream] = {"length": length, "last_msg_age_s": age_s}
        except Exception as exc:
            stats[stream] = {"length": -1, "last_msg_age_s": float("inf"), "error": str(exc)}
    return stats


def check_health() -> dict:
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    stats = get_stream_stats(r)
    stale_streams = [s for s, v in stats.items() if v["last_msg_age_s"] > STALE_THRESHOLD_S]
    dead_streams = [s for s, v in stats.items() if v["last_msg_age_s"] > STALE_THRESHOLD_S * 3]

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "ok" if not stale_streams else ("dead" if dead_streams else "degraded"),
        "streams": stats,
        "stale": stale_streams,
        "dead": dead_streams,
    }

    # Log health event
    HEALTH_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(HEALTH_LOG, "a") as f:
        f.write(json.dumps(result) + "\n")

    # Print dashboard
    print(f"\n{'='*55}")
    print(f"  EXCHANGE HEALTH — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}")
    for stream, info in stats.items():
        age = info["last_msg_age_s"]
        age_str = f"{age:.0f}s ago" if age != float("inf") else "never"
        status = "OK" if age < STALE_THRESHOLD_S else ("DEAD" if age > STALE_THRESHOLD_S * 3 else "STALE")
        print(f"  {stream:<35} {status:<6}  last={age_str}")

    if stale_streams:
        logger.warning(f"Stale streams: {stale_streams}")
        _attempt_restart()
    else:
        logger.success("All exchange streams healthy.")

    print(f"{'='*55}\n")
    return result


def _attempt_restart() -> None:
    """Attempt to restart the collector process."""
    logger.info("Attempting to restart collector...")
    project_root = Path(__file__).resolve().parents[1]
    collector_script = project_root / "collector" / "main.py"

    try:
        # Kill any existing collector
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/FI", "WINDOWTITLE eq cryptofeed-collector"],
                capture_output=True
            )
        else:
            subprocess.run(["pkill", "-f", "collector/main.py"], capture_output=True)

        time.sleep(2)

        # Start new collector
        if sys.platform == "win32":
            subprocess.Popen(
                f'start "cryptofeed-collector" /MIN python {collector_script}',
                shell=True, cwd=str(project_root)
            )
        else:
            subprocess.Popen(
                ["python", str(collector_script)],
                cwd=str(project_root),
                stdout=open(project_root / "logs/collector.log", "a"),
                stderr=subprocess.STDOUT,
            )
        logger.success("Collector restarted.")
    except Exception as exc:
        logger.error(f"Failed to restart collector: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true", help="Run every 5 minutes")
    args = parser.parse_args()

    if args.loop:
        logger.info(f"Exchange health monitor loop started (every {CHECK_INTERVAL_S}s)")
        while True:
            try:
                check_health()
            except Exception as exc:
                logger.error(f"Health check failed: {exc}")
            time.sleep(CHECK_INTERVAL_S)
    else:
        check_health()


if __name__ == "__main__":
    main()
