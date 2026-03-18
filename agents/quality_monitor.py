"""Quality Monitor Agent — runs hourly, checks data quality, alerts on issues.

Uses Claude AI to diagnose problems and suggest fixes.

Usage:
    python agents/quality_monitor.py
    python agents/quality_monitor.py --hours 2 --no-ai
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from loguru import logger

ALERT_LOG = Path("logs/quality_alerts.jsonl")
EXCHANGES = ["binance", "bybit", "okx"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
DATA_TYPES = ["trades", "books", "funding"]


def check_last_n_hours(hours: int = 2) -> list[dict]:
    """Run quality checks on the last N hours of data for all symbols."""
    from validation.quality import DataQualityChecker
    from replay.engine import ReplayEngine

    data_dir = os.getenv("DATA_DIR", "./data")
    checker = DataQualityChecker()
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    issues = []

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            engine = ReplayEngine(data_dir=data_dir)
            trades_df = engine._load("trades", exchange, symbol, start, end)
            books_df = engine._load("books", exchange, symbol, start, end)

            result = {
                "exchange": exchange,
                "symbol": symbol,
                "checked_at": end.isoformat(),
                "window_hours": hours,
                "issues": [],
            }

            # Check trades
            if trades_df.empty:
                result["issues"].append({
                    "type": "no_data",
                    "data_type": "trades",
                    "severity": "high",
                    "detail": f"No trades found for last {hours}h"
                })
            else:
                gap_info = checker.check_gaps(trades_df, expected_interval_ms=1000)
                if gap_info["n_gaps"] > 5:
                    result["issues"].append({
                        "type": "timestamp_gaps",
                        "data_type": "trades",
                        "severity": "medium",
                        "detail": f"{gap_info['n_gaps']} gaps, max={gap_info['max_gap_ms']:.0f}ms"
                    })
                outliers = checker.check_price_outliers(trades_df)
                if outliers > 0:
                    result["issues"].append({
                        "type": "price_outliers",
                        "data_type": "trades",
                        "severity": "high",
                        "detail": f"{outliers} price outliers (>10σ)"
                    })

            # Check books
            if not books_df.empty:
                crossed = checker.check_crossed_book(books_df)
                if crossed > 0:
                    result["issues"].append({
                        "type": "crossed_book",
                        "data_type": "books",
                        "severity": "high",
                        "detail": f"{crossed} rows with best_bid >= best_ask"
                    })
                seq_gaps = checker.check_sequence_gaps(books_df)
                if seq_gaps > 100:
                    result["issues"].append({
                        "type": "sequence_gaps",
                        "data_type": "books",
                        "severity": "low",
                        "detail": f"{seq_gaps} non-consecutive seq numbers"
                    })

            if result["issues"]:
                issues.append(result)

    return issues


def diagnose_with_ai(issues: list[dict]) -> str:
    """Use Claude to diagnose issues and recommend fixes."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "AI diagnosis skipped (no ANTHROPIC_API_KEY set)"

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        issues_text = json.dumps(issues, indent=2)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": f"""You are a senior data engineer at a crypto market data company.
Analyze these data quality issues from the last 2 hours and provide:
1. Root cause diagnosis (1-2 sentences per issue group)
2. Immediate remediation steps
3. Long-term fix recommendation

Issues detected:
{issues_text}

Be concise and technical. Focus on actionable steps."""
            }]
        )
        return message.content[0].text
    except Exception as exc:
        return f"AI diagnosis failed: {exc}"


def write_alert(issues: list[dict], diagnosis: str) -> None:
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    alert = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "issue_count": sum(len(i["issues"]) for i in issues),
        "affected_streams": [f"{i['exchange']}/{i['symbol']}" for i in issues],
        "issues": issues,
        "ai_diagnosis": diagnosis,
    }
    with open(ALERT_LOG, "a") as f:
        f.write(json.dumps(alert) + "\n")
    logger.warning(f"Quality alert written to {ALERT_LOG}")


def print_dashboard(issues: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  QUALITY MONITOR — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    if not issues:
        print("  All streams: OK")
    else:
        total_issues = sum(len(i["issues"]) for i in issues)
        print(f"  Issues found: {total_issues} across {len(issues)} streams")
        for item in issues:
            print(f"\n  {item['exchange']}/{item['symbol']}:")
            for issue in item["issues"]:
                sev = {"high": "[HIGH]", "medium": "[MED]", "low": "[LOW]"}[issue["severity"]]
                print(f"    {sev} {issue['type']}: {issue['detail']}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=2)
    parser.add_argument("--no-ai", action="store_true")
    args = parser.parse_args()

    logger.info(f"Quality Monitor: checking last {args.hours}h...")
    issues = check_last_n_hours(hours=args.hours)
    print_dashboard(issues)

    if issues:
        diagnosis = "AI diagnosis disabled." if args.no_ai else diagnose_with_ai(issues)
        write_alert(issues, diagnosis)
        if not args.no_ai:
            print("=== AI DIAGNOSIS ===")
            print(diagnosis)
            print("=" * 40)
    else:
        logger.success("No quality issues detected.")


if __name__ == "__main__":
    main()
