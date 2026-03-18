"""Daily Report Agent — generates a full quality report for yesterday's data.

Uses Claude AI to write an executive summary.

Usage:
    python agents/daily_report.py
    python agents/daily_report.py --date 2026-03-17
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

REPORTS_DIR = Path("reports/quality")
EXCHANGES = ["binance", "bybit", "okx"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


def generate_report(date_str: str) -> dict:
    """Generate full quality report for a given date."""
    from validation.quality import DataQualityChecker
    data_dir = os.getenv("DATA_DIR", "./data")
    checker = DataQualityChecker()

    report = {
        "date": date_str,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "exchanges": {},
    }

    for exchange in EXCHANGES:
        report["exchanges"][exchange] = {}
        for symbol in SYMBOLS:
            r = checker.generate_report(exchange, symbol, date_str, data_dir)
            report["exchanges"][exchange][symbol] = r

    return report


def write_ai_summary(report: dict) -> str:
    """Use Claude to write a 3-paragraph executive summary."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "_AI summary skipped (no ANTHROPIC_API_KEY)_"

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        # Condense report for prompt
        summary_data = {}
        for ex, symbols in report["exchanges"].items():
            for sym, data in symbols.items():
                key = f"{ex}/{sym}"
                issues = []
                for dtype in ["trades", "books", "funding"]:
                    d = data.get(dtype, {})
                    if d.get("status") == "no_data":
                        issues.append(f"{dtype}: NO DATA")
                    elif d.get("crossed_books", 0) > 0:
                        issues.append(f"{dtype}: {d['crossed_books']} crossed books")
                    elif d.get("gap_check", {}).get("n_gaps", 0) > 5:
                        issues.append(f"{dtype}: {d['gap_check']['n_gaps']} gaps")
                if issues:
                    summary_data[key] = issues

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": f"""You are the Head of Data Quality at CryptoFeed, a crypto market data company.
Write a concise 3-paragraph executive summary for the daily data quality report for {report['date']}.

Issues found:
{json.dumps(summary_data, indent=2) if summary_data else "None — all streams clean."}

Paragraph 1: Overall data quality status for the day.
Paragraph 2: Notable issues (if any) and their business impact.
Paragraph 3: Recommended actions for the engineering team.

Write in professional but direct language."""
            }]
        )
        return message.content[0].text
    except Exception as exc:
        return f"_AI summary failed: {exc}_"


def save_report(date_str: str, report: dict, summary: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"{date_str}.md"

    lines = [
        f"# CryptoFeed Data Quality Report — {date_str}",
        f"\nGenerated: {report['generated_at']}",
        f"\n## Executive Summary\n",
        summary,
        f"\n## Detailed Results\n",
    ]

    for exchange, symbols in report["exchanges"].items():
        lines.append(f"\n### {exchange.title()}")
        for symbol, data in symbols.items():
            lines.append(f"\n#### {symbol}")
            for dtype in ["trades", "books", "funding"]:
                d = data.get(dtype, {})
                status = d.get("status", "unknown")
                n_rows = d.get("n_rows", 0)
                lines.append(f"- **{dtype}**: {status}, {n_rows} rows")
                if d.get("crossed_books", 0) > 0:
                    lines.append(f"  - Crossed books: {d['crossed_books']}")
                if d.get("gap_check", {}).get("n_gaps", 0) > 0:
                    lines.append(f"  - Gaps: {d['gap_check']['n_gaps']}")

    out_path.write_text("\n".join(lines))
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Date YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args()

    if args.date:
        date_str = args.date
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

    logger.info(f"Generating quality report for {date_str}...")
    report = generate_report(date_str)
    summary = write_ai_summary(report)
    out_path = save_report(date_str, report, summary)
    logger.success(f"Report saved: {out_path}")
    print(f"\nExecutive Summary:\n{summary}")


if __name__ == "__main__":
    main()
